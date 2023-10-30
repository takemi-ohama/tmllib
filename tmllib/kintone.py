import requests
import numpy as np
import math
import json
from .etltool import EtlHelper

class Kintone:
    """
    kintone API
    zenkPythonをベースにGPT-4によるリファクタリングと一部機能追加したライブラリです。
    https://github.com/zenk-github/pytone
    """

    BASE_URL_TEMPLATE = 'https://{}.cybozu.com/k/v1/{}'

    def __init__(self, api_token, domain, app):
        self.api_token = api_token
        self.base_url = self.BASE_URL_TEMPLATE.format(domain, '{}')
        self.app = app
        self.headers = {
            "X-Cybozu-API-Token": self.api_token,
            'Content-Type': 'application/json'
        }
        self.property, self.fields = self._get_property()
        self.helper = EtlHelper()

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
        try:
            response = requests.request(method, url, json=json_data, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {str(e)}")

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
        self.helper.parallel(self._update_chunk, args=chunk,chunk=10)
        return

    def _update_chunk(self,params):
        data = {'app': self.app, 'records': list(params)}
        response = self._request_kintone('PUT', 'records.json', json_data=data)
        return response

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
