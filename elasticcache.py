import pdb
import pandas as pd
import redis

class Redis:
    u"""
    Redis接続クラス
    """

    def __init__(self, host = '', db = 0, is_debug=False):
        self.is_debug = is_debug
        self.host = host
        self.__conn = None
        self.__db = db

    @property
    def conn(self):
        if self.__conn is not None:
            return self.__conn
        self.__conn = redis.Redis(host = self.host, port = 6379, db = self.db)
        return self.__conn

    @property
    def db(self):
        return self.__db
        
    @db.setter
    def db(self, value):
        if value != '':
            self.__db = value
            self.__conn = redis.Redis(host = self.host, port = 6379, db = self.__db)
        
    def set(self, key, value):
        self.conn.set(key.value)
        return df
    
    def get(self, key):
        self.conn.get(key.value)
        return df
    
    def mset(self, data):
        return self.conn.mset(data)
    
    def mget(self,keys):
        return self.conn.mget(keys)
    
    def mget_map(self,keys):
         return dict(zip(keys, self.conn.mget(keys)))
    
