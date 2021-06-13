import json
import time
import sys
from lsh import cache, minhash  # https://github.com/mattilyra/lsh


def get_fingerprint():
    """
    TODO
    根据uri和id在数据库查找fingerprint（一个list），不存在则进行计算并存储进数据库
    : [((uri1,id1),(uri2,id2)),...]
    """
    return

def get_jaccard():
    """
    TODO
    根据uri和id找到原始数据文本并计算jaccard相似度，筛选出小于阈值的数据
    :return:
    """
    return

def save_data():
    """
    TODO
    对重复度较高的数据仅保留一份
    :return:
    """
    return

# def add_fingerprints(char_ngram=5, seeds=50, bands=5, hashbytes=4,random_state=7):
#     char_ngram = char_ngram
#     hasher = minhash.MinHasher(seeds=seeds, char_ngram=char_ngram, hashbytes=hashbytes,random_state=random_state)
#     if seeds % bands != 0:
#         raise ValueError('Seeds has to be a multiple of bands. {} % {} != 0'.format(seeds, bands))
#     lshcache = cache.Cache(num_bands=bands, hasher=hasher)
#     line = inputf.readline()
#     while line:
#         line = line.decode('utf8')
#         myline = json.loads(line)
#         if 'fingerprint' in myline:
#             continue
#         mytext = myline['data']
#         fingerprint = hasher.fingerprint(mytext.encode('utf8'))
#         fingerprint = fingerprint.tolist()
#         myline['fingerprint']=fingerprint
#         myline = json.dumps(myline)
#         fp.write(myline)
#         fp.write('\n')
#         line = inputf.readline()