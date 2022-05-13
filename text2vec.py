
import pdb
import re

import numpy as np
import sentencepiece as spm
from gensim.models.word2vec import Word2Vec
from scdv import SparseCompositeDocumentVectors
import time

class Text2Vec:
    u"""
    データ抽出、整形クラス
    dbからデータを抽出して、欠損処理、離散化、ダミー変数化、サンプリング等の処理を行う
    """

    def __init__(self, textfile, model_prefix='sentencepiece'):
        self.input = textfile
        self.model_prefix = model_prefix
        self.sp_model = self.model_prefix + ".model"
        self.sp = None

    def sp_train(self, text):
        """SentencePieceによる分かち書き"""

        print("save training text.")
        np.savetxt(self.input, text, fmt='%s')

        print("SentencePiece Training...")
        time.sleep(1)
        
        spm.SentencePieceTrainer.Train(
            "--input={}, --model_prefix={} \
             --num_threads=32 \
             --character_coverage=0.9995 --vocab_size=32000 \
            ".format(self.input, self.model_prefix)
        )

    def sp_encode(self, text):
        if self.sp is None:
            self.sp = spm.SentencePieceProcessor()
            self.sp.Load(self.sp_model)
        token = self.sp.encode(text, out_type=str)
        token = [re.sub('▁+', '', ' '.join(x)).split(' ') for x in token]
        return token

    def create_scdv_model(self, token):
        w2v = Word2Vec(token, size=64, workers=-1, min_count=5, window=5, sg=1)
        scdv = SparseCompositeDocumentVectors(w2v, 2, 64)
        scdv.get_probability_word_vectors(token)
        return scdv
       
