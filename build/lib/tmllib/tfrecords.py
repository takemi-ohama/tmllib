import os
import pdb
import time
import tensorflow as tf
import concurrent.futures
import numpy as np
import boto3

class Tfrecords:
    u"""
    TfRecordを作成してs3に保存するクラス
    """

    def __init__(self, s3bucket, s3path, chunk=1000, workers=-1):
        self.s3bucket = s3bucket
        self.s3path = s3path
        self.chunk = chunk
        self.workers = workers
        self.s3 = boto3.resource('s3', region_name='ap-northeast-1')

    def parallel(self, arg_list, prefix, future_method=None):
        '''
        チャンク分割+並列処理
        チャンク単位でTFRecordsを作成してS3にアップロード
        '''
        future_method = self.default_download if future_method is None else future_method
        stop = len(arg_list)
        chunk = self.chunk
        #stop = 10
        #chunk = 5
        workers = os.cpu_count() * 10 if self.workers == -1 else self.workers

        # チャンク分割処理
        for suffix, i in enumerate(range(0, stop, chunk)):
            start = time.time()
            tffile = 's3://{}/{}/{}.tfrecords.{:0>3}'.format(self.s3bucket,self.s3path, prefix, suffix)
            print(tffile, end='')
            if tf.io.gfile.exists(tffile):
                print(' -')
                continue
            t = min(i + chunk, stop)
            x_arg_list = arg_list[i:t]
            # 並列処理
            futures = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                for n, args in enumerate(x_arg_list):
                    #futures.append(future_method(args))
                    futures.append(executor.submit(future_method, args))
            print(' .', end='')
            ret = [x.result() if hasattr(x, 'result') else x for x in futures]
            ret = [x for x in ret if x is not None]

            with tf.io.TFRecordWriter(tffile) as f:
                for x in ret:
                    f.write(x)

            end = time.time()
            print('elasped:', '{:.2f}'.format(end - start))

    def default_download(self, args):
        (id, url, label) = args
        try:
            if url.startswith('http'):
                with urllib.request.urlopen(url) as f:
                    image = f.read()
            else:
                p = url.split('/')
                bucket, key = p[2], '/'.join(p[3:])
                image = bucket.Object(key).get()['Body'].read()
        except Exception as e:
            return None

        image = tf.image.decode_jpeg(image, channels=3)
        image = tf.image.resize_with_pad(image, 224, 224)
        image /= 255
        image = np.array(image)

        ret = tf.train.Example(features=tf.train.Features(feature={
            "x": tf.train.Feature(float_list=tf.train.FloatList(value=image.reshape(-1))),
            "y": tf.train.Feature(float_list=tf.train.FloatList(value=[label])),
            "id": tf.train.Feature(int64_list=tf.train.Int64List(value=[id])),
        }))
        return ret.SerializeToString()
