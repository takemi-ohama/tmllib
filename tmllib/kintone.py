import requests
import numpy as np
import math
import json
import time
from .etltool import EtlHelper

class Kintone:
    """
    kintone API
    zenkPythonをベースにGPT-4によるリファクタリングと一部機能追加したライブラリです。
    https://github.com/zenk-github/pytone
    """

    BASE_URL_TEMPLATE = 'https://{}.cybozu.com/k/v1/{}'

    def __init__(self, api_token, domain, app, is_debug=False):
        self.api_token = api_token
        self.base_url = self.BASE_URL_TEMPLATE.format(domain, '{}')
        self.app = app
        self.is_debug = is_debug
        self.headers = {
            "X-Cybozu-API-Token": self.api_token,
            'Content-Type': 'application/json'
        }
        self.property, self.fields = self._get_property()

    def select_all(self, where=None, fields=None, hard_limit=None):
        params = {
            'app': self.app,
            'query': '',
            'totalCount': True,
        }
        if fields is not None:
            params['fields'] = list(set(fields + ['$id', '$revision']))

        records = self._fetch_records_in_batches(params, where, hard_limit)
        records = self._format_records(records)
        return records

    def _request_kintone(self, method, endpoint, json_data=None):
        url = self.base_url.format(endpoint)
        if self.is_debug:
            print(f"[DEBUG] kintone request: {method} {url}")
        try:
            # GETリクエストではparamsを使用、それ以外ではjsonを使用
            if method == 'GET':
                response = requests.request(method, url, params=json_data, headers={'X-Cybozu-API-Token': self.api_token})
            else:
                response = requests.request(method, url, json=json_data, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if self.is_debug:
                if e.response is not None:
                    print(f"[DEBUG] Response status: {e.response.status_code}")
                    print(f"[DEBUG] Response body: {e.response.text}")
                else:
                    print(f"[DEBUG] HTTPError with no response object")
            raise
        except Exception as e:
            if self.is_debug:
                print(f"[DEBUG] Exception: {type(e).__name__}: {str(e)}")
            raise

    def _get_property(self):
        params = {'app': self.app, 'lang': 'default'}
        property = self._request_kintone('GET', 'app/form/fields.json', json_data=params)['properties']
        fields = {y['label']: y for y in property.values()}
        fields |= {
            k: {'type': 'NUMBER', 'code': k, 'label': k, 'required': 'True'}
            for k in ('$id', '$revision')
        }
        return property, fields

    def _fetch_records_in_batches(self, params, where, hard_limit):
        last_rec_id = '0'
        record_count = 0
        all_records = []

        while True:
            query_str = f'($id > {last_rec_id})'
            if where is not None:
                query_str += f' and ({where})'
            params['query'] = f'{query_str} order by $id asc limit 500'

            response = self._request_kintone('GET', 'records.json', json_data=params)
            total_count = int(response['totalCount'])
            record_count += len(response['records'])

            if total_count == 0:
                break

            last_rec_id = response['records'][-1]['$id']['value']
            all_records.append(response['records'])

            if total_count <= 500:
                break
            if record_count >= hard_limit:
                break

        return [record for batch in all_records for record in batch]

    def _format_records(self, records):
        return [
            {self.property[field_code]['label'] if field_code not in (
                '$id', '$revision') else field_code: self._format_field(value) for field_code, value in record.items()}
            for record in records
        ]

    def _format_field(self, field_data):
        field_type = field_data['type']
        field_value = field_data['value']

        if field_type == 'NUMBER' and field_value is not None and field_value != "":
            return self._convert_to_number(field_value)
        elif field_type == 'SUBTABLE':
            return self._format_subtable(field_value)
        else:
            return field_value

    def update(self, records: list):
        """kintone rest apiの制限(updateは1回100件)に従って分割送信"""
        cnt = math.ceil(len(records) / 100)
        chunk = list(np.array_split(records, cnt))
        # 並列処理を無効化して順次処理に変更（レート制限対応）
        for i, c in enumerate(chunk):
            if i > 0:
                time.sleep(1)  # レート制限対策: 1秒待機
            self._update_chunk(c)
        return

    def _update_chunk(self, params):
        # paramsを正しいkintone API形式に変換
        # 既に'record'キーが存在する場合はそのまま使用、ない場合は追加
        records = []
        for param in params:
            param_copy = param.copy()
            if 'record' in param_copy:
                # 既にrecordキーがある場合はそのまま使用
                records.append(param_copy)
            else:
                # recordキーがない場合は追加
                record_id = param_copy.pop('id')
                records.append({
                    'id': record_id,
                    'record': param_copy
                })
        data = {'app': int(self.app), 'records': records}
        if self.is_debug:
            print(f"[DEBUG] update_chunk: app={self.app}, records_count={len(records)}")
            if len(records) > 0:
                print(f"[DEBUG] first record: {records[0]}")

        try:
            response = self._request_kintone('PUT', 'records.json', json_data=data)
            return response
        except requests.exceptions.HTTPError as e:
            # GAIA_RE01: 指定したレコードが見つかりません
            if e.response is not None and e.response.status_code == 404:
                error_body = e.response.json() if e.response.text else {}
                if error_body.get('code') == 'GAIA_RE01':
                    # 存在しないレコードIDを特定してスキップ
                    print(f"[WARNING] Record not found, trying individual updates...")
                    return self._update_records_individually(records)
            raise

    def _update_records_individually(self, records):
        """レコードを1件ずつ更新し、存在しないレコードはスキップする"""
        success_count = 0
        skip_count = 0
        for record in records:
            data = {'app': int(self.app), 'records': [record]}
            try:
                self._request_kintone('PUT', 'records.json', json_data=data)
                success_count += 1
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    print(f"[WARNING] Skipping non-existent record id={record.get('id')}")
                    skip_count += 1
                else:
                    raise
            time.sleep(0.1)  # レート制限対策
        print(f"[INFO] Individual update completed: success={success_count}, skipped={skip_count}")
        return {'success': success_count, 'skipped': skip_count}

    @staticmethod
    def _convert_to_number(value):
        try:
            return int(value)
        except ValueError:
            return float(value)

    @staticmethod
    def _format_subtable(subtable_value):
        formatted_subtable = []

        for sub_rec in subtable_value:
            subtable_record = {'id': sub_rec['id']}

            for sub_field_code, sub_value in sub_rec['value'].items():
                if sub_value['type'] == 'NUMBER' and sub_value['value'] is not None:
                    sub_value['value'] = Kintone._convert_to_number(sub_value['value'])

                subtable_record[sub_field_code] = sub_value['value']
            formatted_subtable.append(subtable_record)

        return formatted_subtable
