import os
import pdb

import boto3
import pandas as pd
import pandas.io.sql as psql
from sqlalchemy import create_engine


class Aurora:
    u"""
    postgresql/redshift接続クラス
    """

    def __init__(self, is_debug=False, target='staging'):
        self.db = None
        self.target = target
        self.is_debug = is_debug

    def con(self):
        if self.target == 'production':
            if self.is_debug: print('connect production')
            self.connect_db()
        elif self.target == 'staging':
            if self.is_debug: print('connect staging')
            self.connect_staging()
        else:
            pass

    def connect_db(self, host, user, passwd, schema):
        if self.db is not None:
            return self
        connector_str = 'mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(
            user, passwd, host, schema)
        self.db = create_engine(connector_str)
        return self

    def read(self, query):
        if self.db is None:
            self.con()
        if self.is_debug: print(query)
        df = psql.read_sql(query, self.db)
        return df

    def execute(self, query):
        if self.db is None:
            self.con()
        if self.is_debug: print(query)
        try:
            with self.db.connect() as con:
                con.execute(query)
        except Exception as e:
            return str(e)
        return "success"

    def update(self, df, target_table, tmp_table):
        df.to_sql(tmp_table, self.db, if_exists='replace', chunksize=10000)
        col = ' '.join(['a.{0} = b.{0}'.format(x) for x in df.columns if x != 'id'])
        query = "update {} a inner join {} b on a.id = b.id set {}".format(target_table, tmp_table, col)
        if self.is_debug: print(query)
        with self.db.connect() as con:
            con.execute(query)

    def undersampling(self, base_query, pos, neg, unique_key, *, ratio=1.0, limit=-1, salt='saltydog', sample_id='sample_id', args={}):
        """
        DBレベルでの不均衡データアンダーサンプリング。
        posiviteデータのratio倍のnegativeデータをランダムに抽出する。

        base_query: SQL文字列でwhere句に{separater}変数を、order by句の位置に{orderby}変数を指定すること
        ex) pos = "status = 'win'"
            neg = "status = 'lose'"
            unique_key = "users.id"
            base_query = "select * from users where deleted_at is null and {separator} {orderby}"
        結果)
         ->(ok) select * from users where deleted_at is null and status = 'win'
         ->(ng) select * from users where deleted_at is null and status = 'lose' order by farm_fingerprint(users.id) limit 2000
        """
        # positiveを取得
        orderby = "order by sha2(concat({},'{}'),224) limit {}".format(
            unique_key, salt, str(limit)) if limit > 0 else ''
        query = base_query.format(separater=pos, orderby=orderby, **args)
        positive = self.read(query)
        pos_num = len(positive)
        if self.is_debug:
            print('positive num:', pos_num)

        # negativeをpositiveのrate倍取得
        limit = round(pos_num * ratio)
        orderby = "order by sha2(concat({},'{}'),224) limit {}".format(unique_key, salt, str(limit))
        query = base_query.format(separater=neg, orderby=orderby, **args)
        negative = self.read(query)
        print('negative num:', str(len(negative)))

        df = pd.concat([negative, positive], ignore_index=True)
        df[sample_id] = df.index
        return df

    def nosampling(self, base_query, condition=' 1=1 '):
        query = base_query.format(separater=condition, orderby='')
        df = self.read(query)
        if self.is_debug:
            print('num:', len(df))
        return df
