import time
import math
import os
import pdb
import pickle
import re
from collections import Counter
from datetime import datetime
import subprocess
import concurrent.futures
from datetime import datetime
from shutil import move
import glob

import pandas as pd
import numpy as np
import pytz
from sklearn.model_selection import train_test_split
import itertools
import boto3


class EtlHelper:
    u"""
    データ分析ETLツール
    """

    def __init__(self, use_cache=True, is_debug=True):
        self.is_debug = is_debug
        self.use_cache = use_cache

    def dump(self, obj, filename):
        with open(filename, 'wb') as f:
            pickle.dump(obj, f, protocol=4)

    def load(self, filename):
        with open(filename, 'rb') as f:
            return pickle.load(f)

    def s3cp(self, src, dest, s3_region=None):
        opt = '--region={}'.format(s3_region) if s3_region is not None else ''
        print(['aws', opt, 's3', 'cp', src, dest])
        subprocess.call(['aws', opt, 's3', 'cp', src, dest])

    def s3sync(self, src, dest, s3_region=None):
        opt = '--region={}'.format(s3_region) if s3_region is not None else ''
        print(subprocess.call(['aws', opt, 's3', 'sync', src, dest]))

    def read_ssm(self, key, region=None):
        #TODO: regionを反映
        client = boto3.client('ssm')
        response = client.get_parameter(
            Name=key,
            WithDecryption=True,
        )
        return response

    # rekognition用のmanifestファイルを作成する
    def make_manifest(self, df, filename):
        dfx = df[df['tvt'] == 'train'].copy()
        dfx['source-ref'] = dfx['image_url']
        dfx[['source-ref']].to_json(filename, orient='records', force_ascii=False, lines=True)

    def display(self, line, *x):
        min_rows = pd.options.display.min_rows
        max_rows = pd.options.display.max_rows
        pd.options.display.min_rows = line
        pd.options.display.max_rows = line
        display(*x)
        pd.options.display.min_rows = min_rows
        pd.options.display.max_rows = max_rows

    def category2id(self, categories):
        tag_idx = sorted(set(categories))
        tag_dic = dict(zip(tag_idx, range(len(tag_idx))))
        return categories.map(tag_dic), tag_idx

    def adjust_frame(self, df, frame, drop=True):
        """
        空フレームを呼び出してカラムをtrain時の構造に一致させる(欠損しているダミーフィールドを補完)
        """
        columns_diff = set(frame.columns) - set(df.columns)
        for x in columns_diff:
            df[x] = frame[x]
        for x in frame:
            df[x] = df[x].astype(frame[x].dtype)

        if drop:
            columns_diff = set(df.columns) - set(frame.columns)
            for x in columns_diff:
                df.drop(x, axis=1, inplace=True)
        return df

    def execute(self, method, filename, **kwargs):
        if self.use_cache and os.path.isfile(filename):
            if self.is_debug: print('using cache.')
            return self.load(filename)

        obj = method(**kwargs) if len(kwargs) > 0 else method()

        if self.use_cache and filename != "":
            if self.is_debug: print('saving cache.')
            d = os.path.dirname(filename)
            if not os.path.isdir(d): os.makedirs(d)
            self.dump(obj, filename)
        return obj

    def parallel(self, future_method, args, *,
                 chunk=2000, workers=-1, executor='thread', cache=None, cache_dir=None, limit=1000000000, wait=0):
        """
        大規模データを使って外部APIを叩く場合のチャンク分割+並列処理
        * 1チャンクごとにThreadまたはProcessでfuture_methodを並列実行
        * 1チャンクごとにキャッシュデータを保存(途中再開可能)可能
        * Google APIのように1分ごとの連続アクセス数制限のあるAPIのためにチャンク単位でウエイト設定可能

        ARGS:
        future_method: 並列実行する関数(引数は配列1つであること)
        args: future_methodに渡す引数の配列
        chunk: 1チャンクごとの処理数
        workers: 1チャンクごとの並列数
        executor: thread/process/debugのいずれかの文字列
        cache: キャッシュする場合のファイル名
        cache_dir: chankごとに別ファイルとなるキャッシュ。
        limit: argsの実行行数(動作テストなどで全部実行しない場合に利用する)
        wait: チャンクごとの最小実行時間(sec)。実際の実行時間がwait値以下の場合、sleepを掛ける
        """
        start = 0
        stop = min(len(args), limit)
        digit = len(str(stop))
        results = []

        # キャッシュ呼び出し
        if cache_dir != None:
            files = sorted(glob.glob(os.path.join(cache_dir, '*.cache')))
            if len(files) > 0:
                # 最後のファイルは壊れている可能性があるのでひとつ前から
                start = (len(files) - 1) * chunk

        elif cache != None and os.path.isfile(cache):
            results = self.load(cache)
            start = len(results)

        if executor == 'thread':
            exec = concurrent.futures.ThreadPoolExecutor
            workers = os.cpu_count() * 5 if workers == -1 else workers
        else:
            exec = concurrent.futures.ProcessPoolExecutor
            workers = os.cpu_count() if workers == -1 else workers

        # チャンク分割処理
        for suffix, i in enumerate(range(start, stop, chunk)):
            if cache_dir != None:
                results = []

            start_time = datetime.now(pytz.timezone('Asia/Tokyo'))
            print(f'{i:0{digit}}-', end='')
            t = min(i + chunk, stop)
            target = args[i:t]

            # 並列処理
            futures = []
            with exec(max_workers=workers) as exe:
                for n, arg in enumerate(target):
                    if executor == 'debug':
                        futures.append(future_method(arg))
                    else:
                        futures.append(exe.submit(future_method, arg))
            ret = [x.result() if hasattr(x, 'result') else x for x in futures]
            results += ret

            # 結果キャッシュ処理
            if cache_dir != None:
                d = os.path.dirname(cache_dir)
                if not os.path.isdir(d): os.makedirs(d)
                self.dump(results, os.path.join(cache_dir, f'{i:07}.cache'))
            elif cache != None:
                d = os.path.dirname(cache)
                if not os.path.isdir(d): os.makedirs(d)
                if os.path.exists(cache):
                    move(cache, cache + '.bak')
                self.dump(results, cache)

            # チャンク終了報告
            end_time = datetime.now(pytz.timezone('Asia/Tokyo'))
            elapse = (end_time - start_time).total_seconds()
            minutes = '{:.0f}m '.format(elapse // 60) if elapse // 60 != 0 else ''
            sec = elapse % 60
            z = t - 1
            print(f'{z:0{digit}}: {minutes}{sec:.3f}s', end='')

            # ウエイト処理
            if wait > 0:
                # wait秒経過までsleepする
                remain = wait - elapse
                if remain > 0:
                    print(' sleep:', int(remain), 'sec', end='')
                    time.sleep(remain)

            print('')

        # チャンク別キャッシュの場合、全体を呼び戻す
        if cache_dir != None:
            files = sorted(glob.glob(os.path.join(cache_dir, '*.cache')))
            results = []
            for x in files:
                results += self.load(x)
        return results

    def train_valid_test_split(self, df, train_size, valid_size=None, stratify=None):
        if valid_size is None:
            valid_size = (1 - train_size) / 2
        remain_valid_size = valid_size / (1 - train_size)
        train, remain = train_test_split(df.copy(), train_size=train_size,
                                         stratify=df[stratify] if stratify in df else None, random_state=13)
        valid, test = train_test_split(remain.copy(), train_size=remain_valid_size,
                                       stratify=remain[stratify] if stratify in df else None, random_state=13)
        return train, valid, test

    def timeline_split(self, df, train_size, valid_size=None, stratify=None, orderby=None):
        if valid_size is None:
            valid_size = (1 - train_size) / 2

        if stratify is not None:
            train, valid, test = [], [], []
            for name, group in df.groupby(stratify):
                tr, va, te = self._split(group, train_size, valid_size, orderby)
                train.append(tr)
                valid.append(va)
                test.append(te)
            train = pd.concat(train)
            valid = pd.concat(valid)
            test = pd.concat(test)
        else:
            train, valid, test = self._split(df, train_size, valid_size, orderby)
        return train, valid, test

    def _split(self, dfx, train_size, valid_size, orderby):
        if orderby is not None: df = dfx.sort_values(orderby)
        maxlen = len(df)
        train_max = math.ceil(maxlen * train_size)
        valid_max = train_max + math.ceil(maxlen * valid_size)
        train = df[0:train_max].sample(frac=1, random_state=0)
        valid = df[train_max:valid_max].sample(frac=1, random_state=0)
        test = df[valid_max:].sample(frac=1, random_state=0)
        return train, valid, test

    def merge_tvt(self, merge_column, train, valid, test):
        train[merge_column] = 'train'
        valid[merge_column] = 'valid'
        test[merge_column] = 'test'
        df = pd.concat([train, valid, test])
        df = df.reset_index(drop=True)
        return df

    def show_posneg_matrix(self, df, dependant, merge_column):
        stat = pd.DataFrame(
            {
                'all': Counter(df[dependant]),
                'train': Counter(df[df[merge_column] == 'train'][dependant]),
                'valid': Counter(df[df[merge_column] == 'valid'][dependant]),
                'test': Counter(df[df[merge_column] == 'test'][dependant]),
            }
        ).T.fillna(0).astype(int)
        print('pos_neg matrix')
        display(stat)

    def freq(self, data, class_width=None):
        data = np.asarray(data)
        if class_width is None:
            class_size = int(np.log2(data.size).round()) + 1
            class_width = round((data.max() - data.min()) / class_size)

        bins = np.arange(0, data.max() + class_width + 1, class_width)
        hist = np.histogram(data, bins)[0]
        cumsum = hist.cumsum()

        return pd.DataFrame(
            {
             '度数': hist,
             '累積度数': cumsum,
             '相対度数': hist / cumsum[-1],
             '累積相対度数': cumsum / cumsum[-1]
             },
            index=pd.Index(
                [f'{bins[i]}-{bins[i+1]}'
                 for i in range(hist.size)],
                name='Class')
        )

    def varplot(self, df):
        '''
        DataFrame内の変数をよい感じに並べて可視化。
        汎用化するには、描画が重くなるunique(つまりID)な列への対応が必要。
        '''
        import matplotlib.dates as mdates
        import matplotlib.pyplot as plt
        import matplotlib.ticker as ticker
        import seaborn as sns

        max_col = 5
        max_row = math.ceil(len(df.columns) / max_col)
        fig, ax = plt.subplots(max_row, max_col, figsize=[20, 3.7 * max_row])

        r = 0; c = 0
        for name in df:
            if df[name].dtype == 'datetime64[ns]':
                x = df.set_index(name, drop=False)[name].dropna().resample('D').count()
                ax[r][c].plot(x)
                ax[r][c].set_xlabel(name)
                ax[r][c].get_xaxis().set_major_formatter(mdates.DateFormatter('%Y/%m/%d'))
                ax[r][c].get_xaxis().set_major_locator(ticker.MaxNLocator(integer=True, nbins=3))
            elif name.endswith('_id'):
                ax[r][c].text(0.5, 0.5, name + ' is ID', horizontalalignment='center', verticalalignment='center')
                ax[r][c].set_xlabel(name)
            elif df[name].dtype == 'object':
                sns.countplot(df[name].fillna(False), ax=ax[r][c])
            else:
                sns.distplot(df[name].dropna(), ax=ax[r][c])
            # 位置情報のインクリメント
            (c, r) = (0, r + 1) if (c == max_col - 1) else (c + 1, r)
        return fig


class Pipeline:
    """
    キャッシュ機能付き連続実行
    辞書の配列で実行する処理群を記載
    辞書内のキーとして、method, cachename, args, kwargs を指定可能
      name: 処理名(必須)
      method: 実行する関数ポインタ(必須)
      return: 戻り値の変数名
      cache: cacheファイル名。省略するとここではキャッシュを作らない
      args: methodの引数配列
      kwargs: methodの名前付き引数辞書
    cacheが指定されていてそのファイルが存在した場合、それ以前の処理はスキップされる
    args, kwargsに '#'で囲まれた変数名を指定すると、それ以前の処理で生成された戻り値を利用できる
    ---
    example:
    steps = [
            {
                'name': 'transform',
                'method': self.loader.transform,
                'cache': True,
                'return': ('df'),
                'args': [df],
            },
            {
                'name': 'preprocess',
                'method': self.loader.preprocess,
                'return': ('df', 'categories'),
                'args': ['#df#'],
            },
    ]
    """

    def __init__(self, steps, use_cache=True,
                 is_debug=True, cache_dir='./cache',
                 returns=[],
                 ):
        self.steps = steps
        self.is_debug = is_debug
        self.use_cache = use_cache
        self.cache = cache_dir + '{}.cache'
        self.returns = returns

        self.vars = re.compile('^\#.*\#$')
        self.helper = Helper()

    def run(self, breakpoint=None):
        steps = self.steps
        ret = {}

        # キャッシュ存在チェックと呼び出し
        start = 0
        for i, x in self.enumerate_reversed(steps):
            cache = self.cache.format(x['name'])
            if self.use_cache and 'cache' in x and x['cache'] and os.path.exists(cache):
                # キャッシュがあった場合呼び出し
                print('recent cache: ', cache)
                ret = self.helper.load(cache)
                start = i + 1
                break;

        # キャッシュを呼び出したところ以降から処理を連続実行
        breakpoint = max(min(len(steps), breakpoint), start) if breakpoint is not None else len(steps)
        for x in steps[start:breakpoint]:

            print('start:', x['name'], '-------------------')
            start = datetime.now(pytz.timezone('Asia/Tokyo'))

            args = x['args'] if 'args' in x and len(x['args']) > 0 else []
            args = [self.parse_arg(y, ret) for y in args]

            kwargs = x['kwargs'] if 'kwargs' in x and len(x['kwargs']) > 0 else {}
            kwargs = {k: self.parse_arg(y, ret) for k, y in kwargs.items()}

            obj = x['method'](*args, **kwargs)
            ret = self.parse_return(x, obj, ret)

            if self.use_cache and 'cache' in x and x['cache']:
                cache = self.cache.format(x['name'])
                d = os.path.dirname(cache)
                if not os.path.isdir(d): os.makedirs(d)
                self.helper.dump({k: ret[k] for k in self.returns if k in ret}, cache)

            end = datetime.now(pytz.timezone('Asia/Tokyo'))
            self.print(start, end, 'done : {} ----------'.format(x['name']))

        return tuple([ret[x] if x in ret else None for x in self.returns]) if len(self.returns) > 1 else ret[self.returns[0]]

    def enumerate_reversed(self, lyst):
        length = len(lyst) - 1
        for index, value in enumerate(reversed(lyst)):
            index = length - index
            yield index, value

    def parse_return(self, x, obj, ret):
        if 'return' not in x:
            return ret
        if type(x['return']) is str:
            ret[x['return']] = obj
            return ret
        if len(x['return']) == 1:
            ret[x['return'][0]] = obj
            return ret
        for i, y in enumerate(x['return']):
            ret[y] = obj[i]
        return ret

    def parse_arg(self, arg, ret):
        if type(arg) is str and self.vars.match(arg):
            varname = arg.replace("#", '')
            return ret[varname]
        return arg

    def print(self, start: datetime, end: datetime, name=None):
        elapse = (end - start).total_seconds()
        min = '{:.0f}m '.format(elapse // 60) if elapse // 60 != 0 else ''
        sec = elapse % 60
        print('{}: {}{:.3f}s'.format(name, min, sec))


class StopWatch:
    """
    実行時間計測
    """

    def __init__(self):
        self.start = datetime.now(pytz.timezone('Asia/Tokyo'))
        print('start:', self.start.strftime('%Y/%m/%d %H:%M:%S %Z'))
        self.pre = self.start
        self.i = 0

    def stop(self):
        end = datetime.now(pytz.timezone('Asia/Tokyo'))
        if self.pre != self.start:
            self.split('last')
        self.print(self.start, end, 'total')
        print("done:", end.strftime('%Y/%m/%d %H:%M:%S %Z'))

    def split(self, name='elapse'):
        self.i += 1
        end = datetime.now(pytz.timezone('Asia/Tokyo'))
        self.print(self.pre, end, "{:02d}_{}".format(self.i, name))
        self.pre = end

    def print(self, start: datetime, end: datetime, name=None):
        elapse = (end - start).total_seconds()
        min = '{:.0f}m '.format(elapse // 60) if elapse // 60 != 0 else ''
        sec = elapse % 60
        print('{}: {}{:.3f}s'.format(name, min, sec))
