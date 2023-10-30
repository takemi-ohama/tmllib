import json
import pdb

import pandas as pd
from google.oauth2 import service_account
import boto3
import os
from google.cloud import bigquery
from .config_abc import BaseConfig


class BigQuery:
    u"""
    Google BigQuery接続クラス
    """

    def __init__(self, conf: BaseConfig):
        self.conf = conf
        self.account_type = self.conf.account_type
        self.project_id = self.conf.project_id if hasattr(self.conf, 'project_id') else None
        self.json_key = self.conf.json_key if hasattr(self.conf, 'json_key') else None
        self.region = self.conf.aws_region if hasattr(self.conf, 'aws_region') else None
        self._cred = None
        self.client = None

    def read_gbq(self, query, args={}):
        query = query.format(**args)
        if self.conf.is_debug: print(query)
        df = pd.read_gbq(query, project_id=self.project_id, dialect='standard', credentials=self.cred())
        return df

    def write_gbq(self, df, tablename, table_schema=None, location=None, if_exists='replace'):
        df.to_gbq(tablename, project_id=self.project_id, if_exists=if_exists,
                  credentials=self.cred(), table_schema=table_schema, location=location)
        return df

    def cred(self):
        if self._cred is not None:
            return self._cred
        if self.account_type == 'file':
            self._cred = service_account.Credentials.from_service_account_file(self.json_key)
        elif self.account_type == 'ssm':
            json_key = json.loads(boto3.client('ssm', region_name=self.region).get_parameter(
                Name=self.json_key, WithDecryption=True)['Parameter']['Value'])
            self._cred = service_account.Credentials.from_service_account_info(json_key)
        elif self.account_type == 'env':
            self._cred = service_account.Credentials.from_service_account_file(
                os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
        self.client = bigquery.Client(project=self.project_id, credentials=self._cred)
        return self._cred

    def jsoncolumn_to_df(self, data, prefix=None):
        lst = data.values.tolist()
        json_str = '[' + ','.join(lst) + ']'
        df_s = pd.read_json(json_str)
        if prefix != None:
            prefix = prefix + '_'
            df_s.columns = [prefix + x for x in df_s.columns]
        return df_s

    def undersampling(self, base_query, pos, neg, unique_key, ratio=1.0, limit=-1, args={}):
        """
        DBレベルでの不均衡データアンダーサンプリング。posのデータ数のratio倍のnegデータをランダムに抽出する。
        ここではnegativeを5-10倍出してコード側でSMOTEENNを掛けるなどの操作を推奨

        base_query: SQL文字列でwhere句に{separater}変数を、order by句の位置に{orderby}変数を指定すること
        ex) pos = "status = 'win'"
            neg = "status = 'lose'"
            unique_key = "users.id"
            base_query = "select * from users where deleted_at is null and {separator} {orderby}"

         ->(ok) select * from users where deleted_at is null and status = 'win'
         ->(ng) select * from users where deleted_at is null and status = 'lose' order by farm_fingerprint(users.id) limit 2000
        """
        # positiveを取得
        orderby = 'order by farm_fingerprint({}) limit {}'.format(unique_key, str(limit)) if limit > 0 else ''
        query = base_query.format(separater=pos, orderby=orderby, **args)
        positive = self.read_gbq(query)
        pos_num = len(positive)
        if self.conf.is_debug:
            print('positive num:', pos_num)

        # negativeをpositiveのrate倍取得
        limit = round(pos_num * ratio)
        orderby = 'order by farm_fingerprint({}) limit {}'.format(unique_key, str(limit))
        query = base_query.format(separater=neg, orderby=orderby, **args)
        negative = self.read_gbq(query)
        print('negative num:', str(len(negative)))

        df = pd.concat([negative, positive], ignore_index=True)
        return df

    def nosampling(self, base_query, condition=' 1=1 '):
        query = base_query.format(separater=condition, orderby='')
        df = self.read_gbq(query)
        if self.conf.is_debug:
            print('num:', len(df))
        return df

    def upload_csv(self, filename, tablename, **kwarg):
        df = pd.read_csv(filename, **kwarg)
        self.write_gbq(df, tablename)

    def query_with_noreturn(self, sql):
        self.cred()
        if self.conf.is_debug: print(sql)
        self.client.query(sql).result()

    def query(self, sql):
        self.cred()
        if self.conf.is_debug: print(sql)
        return self.client.query(sql).to_dataframe()
