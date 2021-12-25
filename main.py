#!/usr/bin/env python
import csv
import logging
import os
import requests
from collections import OrderedDict
from io import StringIO
from hashlib import md5


logging.basicConfig()
logger = logging.getLogger("keg")
logger.setLevel(logging.DEBUG)
log = logger.info
debug = logger.debug


def split_hash( hash ):
	"""
	해시코드를 [2:2:all]로 쪼개서 반환
	"""
	return hash[0:2], hash[2:4], hash


class ServerError( Exception ):
	"""
	서버에러
	"""
	pass


class ServerConfigurationError( ServerError ):
	"""
	서버설정에러
	"""
	pass


class FlatINI( OrderedDict ):
	"""
	하나의 키에 여러개의 값을 설정할수 있는 딕셔너리
	An OrderedDict with optional multiple values per key.
	Can read from a "flat ini" file.
	"""
	def readfp( self, f ):
		"""
		설정파일로 부터 설정값 로딩
		"""
		for line in f.readlines():
			line = line.strip()
			if not line or line.startswith("#"):
				continue
			key, sep, value = line.partition("=")
			key = key.strip()
			value = value.strip()
			self[key] = value.strip()

	def __setitem__( self, key, value ):
		"""
		설정값 쓰기
		"""
		if key in self:
			if not isinstance( self[key], list ):
				super().__setitem__( key, [ self[ key ] ] )
			self[key].append( value )
		else:
			super().__setitem__( key, value )

	def items( self ):
		"""
		[키,값] 목록 조회
		"""
		for k, v in super().items():
			if isinstance( v, list ):
				for item in v:
					yield k, item
			else:
				yield k, v

	def keys( self ):
		"""
		[키] 목록 조회
		"""
		for k, v in super().items():
			if isinstance( v, list ):
				for item in v:
					yield k
			else:
				yield k

	def values( self ):
		"""
		[값] 목록 조회
		"""
		for k, v in super().values():
			if isinstance( v, list ):
				for item in v:
					yield item
			else:
				yield v

	def __str__( self ):
		return "\n".join( "{} = {}".format( k, v ) for k, v in self.items() )


class NGDPCache:
	"""
	로컬 파일 저장소
	"""
	HOME 			= os.path.expanduser("~") # "C:\\Users\\XXXXXX"
	XDG_CACHE_HOME 	= os.environ.get( "XDG_CACHE_HOME", os.path.join( HOME, ".cache" ) ) # "C:\\Users\\XXXXXX\\.cache"

	def __init__( self, domain: str, basedir: str = XDG_CACHE_HOME ):
		self.domain 	= domain
		self.basedir 	= os.path.join( basedir, domain )

	def contains( self, key, name ):
		"""
		base/key/name 경로에 로컬파일 존재 여부
		"""
		path = os.path.join( self.basedir, key, name )
		return os.path.exists( path )

	def get( self, key, name ):
		"""
		base/key/name 경로로 로컬파일 로딩
		:param key config, version 등...
		:param name HEX16 해시
		"""
		path = os.path.join( self.basedir, key, name )
		with open( path, "rb" ) as f:
			return f.read()

	def write( self, key, name, data, hash ):
		"""
		base/key/name 경로로 로컬파일로 저장
		"""
		debug( "write_to_cache( key=%r, name=%r, data=%r, hash=%r", key, name, len( data ), hash )
		dirname = os.path.join( self.basedir, key )
		if not os.path.exists( dirname ):
			# debug("mkdir %r", dirname)
			os.makedirs( dirname )
		fname = os.path.join( dirname, name )
		with open( fname, "wb" ) as f:
			f.write( data )
		log( "Written %i bytes to %r" % ( len( data ), fname ) )


class NGDPConnection:
	"""
	Next Generation Distribution Platform = Trustted Application Content Transfer ( Web ) + CASC ( Local )
	https://wowdev.wiki/NGDP
	"""
	def __init__( self, url: str, region: str ="kr" ):
		self.host 			= url.format( region = region )
		self.region 		= region
		self.cache 			= NGDPCache("info.hearthsim.keg")
		self._obj_cache 	= {}
		self._cdn_host 		= None
		self._build_config 	= None
		self.verify 		= False

	@property
	def cdns( self ):
		return self._get_cached_csv( "/cdns" )

	@property
	def cdn( self ):
		if not self._cdn_host:
			cdns = self.cdns
			if not cdns:
				raise ServerConfigurationError( "No CDN available" )
			for cdn in cdns:
				if cdn["Name"] == self.region:
					break
			else:
				cdn = cdns[0]
			cdn_host = cdn["Hosts"].split(" ")[0]

			self._cdn_host = "http://{cdn}/{path}/".format( cdn=cdn_host, path=cdn["Path"] )

		return self._cdn_host

	@property
	def versions( self ):
		for row in self._get_cached_csv( "/versions" ):
			if row["Region"] == self.region:
				row["BuildConfig"] = self.get_config( row["BuildConfig"] )
				row["CDNConfig"]   = self.get_config( row["CDNConfig"] )
				yield row

	def _parse_csv( self, rows ):
		"""
		설정파일 파싱
		:param rows csv.reader()로 반환된 iterator
		"""
		rows 			= list( rows )
		columns 		= rows[0]
		column_names 	= [ c.split("!")[0] for c in columns ] # 칼럼이름!칼럼타입:숫자 | 칼럼이름!칼럼타입:숫자 | ...

		ret 			= []
		for row in rows[1:]:
			ret.append( { k: v for k, v in zip( column_names, row ) } )

		return ret

	def _get_cached_csv( self, path: str ):
		"""
		패치서버로 부터 특정경로 설정파일 다운로드 및 캐싱된 파일 데이터 리턴
		"""
		if path not in self._obj_cache:
			res 	= self.get( path )
			hash 	= md5( res.content ).hexdigest()
			self.cache.write( "cdns", hash, res.content, hash )

			reader 	= csv.reader( StringIO( res.text ), delimiter="|" )  # 칼럼값 | 칼럼값 | ...
			self._obj_cache[path] = self._parse_csv( reader )

		return self._obj_cache[path]

	def get_or_cache( self, key, hash, name=None ):
		"""
		웹에서 다운로드 및 로컬캐시 로딩
		"""
		if name is None:
			name = hash

		if not self.cache.contains( key, name ):
			data = self.cdn_get( "{0}/{1}/{2}/{3}".format( key, *split_hash( name ) ) )
			self.cache.write( key, name, data, hash )
		else:
			data = self.cache.get( key, name )

		return data

	def get_config( self, hash ):
		"""
		/config/해시 파일 로딩
		"""
		data 	= self.get_or_cache( "config", hash )
		config 	= FlatINI()
		config.readfp( StringIO( data.decode( "utf-8" ) ) )
		return config

	def get_data( self, hash ):
		"""
		archive 다운로드
		"""
		index 	= self.get_or_cache( "data", hash, name = hash + ".index" )
		data 	= self.get_or_cache( "data", hash )
		return index, data

	def get_patch( self, hash ):
		data 	= self.get_or_cache( "patch", hash )
		return data

	def cdn_get( self, path: str ):
		"""
		cdn/path 경로로 웹 다운로드
		"""
		url = self.cdn + path
		debug( "[정보] GET %s", url )
		res = requests.get( url )
		if res.status_code != 200:
			raise ServerError( "Got HTTP %r when querying %r" % ( res.status_code, url ) )
		return res.content

	def get( self, path ):
		"""
		호스트로 부터 특정 경로 API 요청
		'http://kr.patch.battle.net:1119/wow' + '/versions'
		"""
		return requests.get( self.host + path )


def main():
	"""
	메인 진입점 함수
	"""
	url 				= "http://{region}.patch.battle.net:1119/wow"
	region 				= "kr"
	conn 				= NGDPConnection( url, region )
	conn.cache.basedir  = "./cache"

	for v in conn.versions:
		build 			= v["BuildId"]
		build_name 		= v["VersionsName"]
		print( "Found build %s (%r)" % ( build_name, build ) )

		for archive in v["CDNConfig"]["archives"].split(" "):
			conn.get_data( archive )

		patch_ekey = v["BuildConfig"]["patch"]
		conn.get_patch( patch_ekey )

		patch_config = conn.get_config( v["BuildConfig"]["patch-config"] )
		assert patch_config["patch"] == patch_ekey


if __name__ == "__main__":
	main()
