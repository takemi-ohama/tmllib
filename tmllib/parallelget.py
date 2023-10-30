import os
import hashlib
import pdb
import requests
from .etltool import EtlHelper

class Downloader:

    def __init__(self,cache_dir='/home/sagemaker-user/SageMaker/storage/image_cache'):

        #with共通で一つの画像キャッシュを持つ
        self.cache_dir = cache_dir
        self.helper = EtlHelper()

    def download_files(self, urls):
        files = self.helper.parallel(self.download, urls)
        return files

    def download(self, url):
        if url is None:
            return None

        _, ext = os.path.splitext(url)
        basename = hashlib.sha256(url.encode('utf-8')).hexdigest() + ext
        subdir = basename[:3]
        filename = os.path.join(self.cache_dir, subdir, basename)
        os.makedirs(os.path.join(self.cache_dir, subdir), exist_ok=True)

        if os.path.exists(filename):
            if os.path.getsize(filename) == 0:
                return '#forbidden'
            return filename

        try:
            r = requests.get(url)
            if r.status_code == requests.codes.forbidden:
                with open(filename, 'w') as f:
                    f.write('')
                return '#forbidden'
            r.raise_for_status()
            with open(filename, 'wb') as f:
                f.write(r.content)
        except Exception as e:
            print(e)
            raise e
        return filename
