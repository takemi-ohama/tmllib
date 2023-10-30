
import json
import boto3
import pandas as pd
import hashlib
from google.api_core.exceptions import NotFound
from .config_abc import BaseConfig
from .etltool import EtlHelper
from .bigquery import BigQuery
from .kintone import Kintone

class BQKintone:

    def __init__(self, conf: BaseConfig, tables):
        self.conf = conf
        self.apps = json.loads(
            boto3.client('ssm', region_name=self.conf.aws_region)
            .get_parameter(Name=self.conf.app_list, WithDecryption=True)
            ['Parameter']['Value']
        )
        self.db = BigQuery(conf)
        self.helper = EtlHelper()
        self.schema_type = 'type'

        # フィールド名変換辞書。結局ほぼ全部_に変換したので、setで十分かも。
        self.ng_fields = str.maketrans({
            '!': '_', '"': '_', '$': '_', '(': '_', ')': '_', '*': '_', ',': '_',
            '.': '_', '/': '_', ';': '_', '?': '_', '@': '_', '[': '_', '\\': '_',
            ']': '_', '^': '_', '`': '_', '{': '_', '}': '_', '~': '_', '【': '_',
            '】': '_', '−': '_', '（': '_', '）': '_', '・': '_', '、': '_',
            '\ufeff': '', '※': '_', '。': '_', '：': '_', '　': '_',
        })

        # [{kintoneアプリ名: bigquertテーブル名},{...}...]
        self.tables = tables

    def run(self):
        """idで全テーブル取得"""
        for k, v in self.tables.items():
            last_id = self._get_last_id(v)
            where = f"$id > {last_id}"
            self.etl(v, k, where)

    def insert_as_updated(self, appname):
        tablename = self.tables[appname]
        last_updated_at = self._get_last_updated_at(tablename)
        where = f'更新日時 > "{last_updated_at}"'
        self.etl(tablename, appname, where)

    def insert_as_id(self, appname):
        tablename = self.tables[appname]
        last_id = self._get_last_id(tablename)
        where = f"$id > {last_id}"
        self.etl(tablename, appname, where)

    def update_kintone(
        self, app_name,
        id: pd.Series,
        values: pd.DataFrame,
        field_codes=None
    ):
        """kintone側の情報を更新する"""
        # fields_codeとpandas.columnsの対応表を作成
        col_dic = (
            {x: y for x, y in zip(values.columns, field_codes)}
            if field_codes is not None else {x: x for x in values.columns}
        )

        # kintone apiのrecordsオブジェクトを作成
        # ref: https://cybozu.dev/ja/kintone/docs/rest-api/records/update-records/
        t = values.T
        records = [
            {'id': int(x),
             'record': {col_dic[y]:{'value': x} for x, y in zip(t[i], t[i].index)}
             }
            for i, x in enumerate(id)
        ]

        info = self.apps[app_name]
        app = Kintone(info['api_token'], info['sub_domain'], info['app_id'])
        res = app.update(records)
        return res

    def etl(self, tablename, appname, where):
        '''
        kintoneからbigqueryへのデータ転送処理
        Parquetの制約が厳しいので、日本語フィールド名を一度md5に変換してテーブルを作成。
        その後alter table rename columnで日本語フィールド名に戻す。
        '''

        # kintoneからデータを取得
        df, fields = self._select(app_name=appname, where=where, limit=10000)
        if len(df) == 0:
            print(appname, 'to', tablename, ':', len(df))
            return

        # フィールド名をMD5に変換し、辞書を保存。
        col_utf8, fields_md5 = self._hash_fields(df, fields)

        # スキーマを生成してframeを型変換
        schema = self._create_schema(df, fields_md5)
        df = self._adjust_type(df, schema)

        # bigquerryにtmp書き込み
        self.db.write_gbq(df, tablename=tablename + '_tmp2', table_schema=schema)

        # numeric型と日付型を型変換
        self._change_type_bq(tablename, fields_md5)

        # フィールド名をクリーニング
        col_utf8 = self._clean_fieldname(col_utf8)

        # bigqueryのフィールド名を日本語に変更
        sql = self._rename_sql(tablename + '_tmp', df.columns, col_utf8)
        self.db.query_with_noreturn(sql)

        # insert intoに渡すフィールド一覧を作成
        tmp_fields = self._get_fieldnames(f'{tablename}_tmp')
        fields = "`" + "`, `".join(tmp_fields['column_name'].to_list()) + "`"

        # 存在しないフィールドを検知したときはalter table add columnを実行
        self._add_columns(tablename, tmp_fields)

        # tmpからオリジナルにコピー
        self._insert_original(tablename, fields)

        # id,revisionが同じレコードは削除
        self._drop_duplicated(tablename)

        # tmpを削除
        sql = f"drop table {tablename}_tmp; drop table {tablename}_tmp2"
        self.db.query_with_noreturn(sql)

        # report
        print(appname, 'to', tablename, ':', len(df))

    def _drop_duplicated(self, tablename):
        """id,revisionが同じレコードは削除"""
        sql = f"""
            merge {tablename} table
            using (
                select
                    id,revision,
                    min(inserted_at) as inserted_at
                from
                    {tablename}
                group by
                    id,revision
                having count(id) > 1
            ) duplicated_origin
            on  table.id = duplicated_origin.id and table.revision = table.revision
            and table.inserted_at != duplicated_origin.inserted_at
            when matched then
                delete
            ;
        """
        self.db.query_with_noreturn(sql)

    def _insert_original(self, tablename, fields):
        """tmpテーブルからoriginalテーブルへのinsert"""
        try:
            sql = f"insert into {tablename} ({fields},`inserted_at`) select *,current_timestamp() from {tablename}_tmp"
            self.db.query_with_noreturn(sql)
        except NotFound as e:
            sql = f"create table {tablename} clone {tablename}_tmp;"
            self.db.query_with_noreturn(sql)

    def _add_columns(self, tablename, tmp_fields):
        """originalにないフィールドを検出した際にフィールドをalter table add columnする"""
        org_fields = self._get_fieldnames(f'{tablename}')
        tmp_set = set(tmp_fields['column_name'].to_list())
        org_set = set(org_fields['column_name'].to_list())
        tmp_type = {x: y for x, y in zip(tmp_fields['column_name'], tmp_fields['data_type'])}

        # 差集合を取って新フィールドを検出
        diff = tmp_set - org_set
        if len(diff) == 0: return

        # 新フィールドの型を取得してsqlを作成
        ret = [(x, tmp_type[x]) for x in diff]
        add_col = ', '.join([f"add column `{name}` {type}" for name, type in ret])
        sql = f"alter table {tablename} {add_col}"
        self.db.query_with_noreturn(sql)

    def _get_fieldnames(self, tablename):
        """"フィールド名と型の一覧をdataframeで返す"""
        dataset, table = tablename.split('.')
        sql = f"""
            select column_name,data_type,
            from {dataset}.INFORMATION_SCHEMA.COLUMNS
            where table_name='{table}'
            order by ordinal_position
        """
        df = self.db.read_gbq(sql)
        return df

    def _hash_fields(self, df, fields):
        """parquetの制約を回避するため、一旦フィールド名を全てmd5ハッシュにする"""
        def md5(x): return hashlib.md5(x.encode()).hexdigest()
        col_utf8 = df.columns
        df.columns = df.columns.map(md5)
        fields_md5 = {md5(k): v for k, v in fields.items()}
        return col_utf8, fields_md5

    def _get_last_updated_at(self, tablename):
        """bigquery転送済みデータの最新更新日次を返す"""
        sql = f"select max(`更新日時`) as last_updated_at from {tablename}"
        try:
            df = self.db.query(sql)
        except NotFound as e:
            return 0
        last_updated_at = df['last_updated_at'][0].strftime('%Y-%m-%dT%H:%M:%SZ') if len(df) > 0 else '1900-01-01'
        return last_updated_at

    def _get_last_id(self, tablename):
        """bigquery転送済みデータの最新更新idを返す"""
        sql = f"select max(`id`) as id from {tablename}"
        try:
            df = self.db.query(sql)
        except NotFound as e:
            return 0
        id = df['id'][0] if len(df) > 0 else 0
        return round(id)

    def _change_type_bq(self, tablename, fields):
        """tmpテーブルのnumber型と日付型を型変換する"""
        tmpl = (
            "drop table if exists " + tablename + "_tmp; "
            "create table " + tablename +
            "_tmp as select * except({0}), {1} from " +
            tablename + "_tmp2;"
        )
        excepts = ', '.join([
            f"`{k}`" for k, x in fields.items()
            if x['type'] in ('RECORD_NUMBER', 'NUMBER', 'DATETIME')
        ])
        casts = "safe_cast(`{0}` as {1}) as `{0}`"

        type_dic_bq = {
            'NUMBER': 'Numeric',
            'RECORD_NUMBER': 'Numeric',
            'DATETIME': 'Timestamp',
        }
        casts = ', '.join([
            casts.format(k, type_dic_bq[x['type']]) for k, x in fields.items()
            if x['type'] in ('RECORD_NUMBER', 'NUMBER', 'DATETIME')
        ])
        self.db.query_with_noreturn(tmpl.format(excepts, casts))

    def _rename_sql(self, table_name, old_names, new_name):
        """bigqueryのフィールド名を一括変換する"""
        cmd = f'alter table {table_name} '
        rename = [f'rename column `{x}` to `{y}`' for x, y in zip(old_names, new_name)]
        sql = cmd + ', '.join(rename) + ';'
        return sql

    def _select(self, app_name, where=None, fields=None, limit=None):
        """kintoneの情報を取得する"""
        info = self.apps[app_name]
        app = Kintone(info['api_token'], info['sub_domain'], info['app_id'])
        res = app.select_all(where, fields, hard_limit=limit)
        return pd.DataFrame(res), app.fields

    def _create_schema(self, df, fields):
        '''
        フィールド定義からbiqrueryのスキーマを生成する
        が、Parquetのチェックが厳しすぎて転送できないのでほぼStringで送って
        bigquery側で型変換を行う羽目になった。
        '''
        type_dic = {
            'RADIO_BUTTON': 'bool',

            # numeric(bigqueryに転送後にalter tableで型変換)
            'NUMBER': 'String',
            'RECORD_NUMBER': 'String',

            'SINGLE_LINE_TEXT': 'String',
            'DROP_DOWN': 'String',
            'MULTI_LINE_TEXT': 'String',
            'LINK': 'String',

            # json
            'CHECK_BOX': 'String',
            'USER_SELECT': 'String',
            'MULTI_SELECT': 'String',
            'SUBTABLE': 'String',
            'MODIFIER': 'String',
            'CREATOR': 'String',

            'DATE': 'DATE',
            'UPDATED_TIME': 'TIMESTAMP',
            'DATETIME': 'TIMESTAMP',
            'CREATED_TIME': 'TIMESTAMP',
            'CALC': 'string',  # TODO
        }

        schema = [
            {
                'name': k, self.schema_type: type_dic[fields[k]['type']],
                'mode':self._mode(fields[k])
            }
            for k in df.columns
        ]
        return schema

    def _mode(self, f):
        ret = 'NULLABLE' if 'required' in f and (f['required'] is None or f['required'] == True) else ""
        return ret

    def _adjust_type(self, df, schema):
        '''
        データ型に基づいてdataframeの型を変換する
        '''
        type_conversion = {
            'bool': (bool, None),
            'BigNumeric': (float, None),
            'Integer': (int, None),
            'DATETIME': (None, pd.to_datetime),
            'TIMESTAMP': (None, pd.to_datetime),
            'DATE': ('datetime64[ns]', pd.to_datetime),
            'String': (str, None)
        }

        for x, s in zip(df.columns, schema):
            dtype, converter = type_conversion.get(s[self.schema_type], (None, None))

            # データ型がマッピングに存在する場合、変換を実施
            if dtype is not None:
                df[x] = df[x].replace('', None)
                df[x] = converter(df[x].astype(dtype)) if converter else df[x].astype(dtype)
            elif converter is not None:
                df[x] = converter(df[x])

        return df

    def _clean_fieldname(self, columns):
        ''':
        bigqueryの規約に従って日本語フィールド名をクリーニングする.
        またid,revisionは_を取っておく。
        '''
        col = [x.translate(self.ng_fields) for x in columns]
        col = [x if x not in ('_id', '_revision') else x.replace('_', '') for x in col]
        return col
