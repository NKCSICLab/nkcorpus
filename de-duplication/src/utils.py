import copy
import itertools
import json
from collections import defaultdict

from lsh import minhash  # https://github.com/mattilyra/lsh


def get_candidate_pairs(all_data, char_ngram=5, seeds=50, bands=5, hashbytes=4):
    """
    todo 内存不够可以将data字段清除，需要时从文件读取
    :param all_data:
    :param char_ngram:
    :param seeds:
    :param bands:
    :param hashbytes:
    :return:
    """
    if seeds % bands != 0:
        raise ValueError('Seeds has to be a multiple of bands. {} % {} != 0'.format(seeds, bands))
    bands_length = seeds // bands
    hasher = minhash.MinHasher(seeds=seeds, char_ngram=char_ngram, hashbytes=hashbytes, random_state=8787)

    hash_bin = [defaultdict(set) for _ in range(bands)]
    candidate_pairs = set()
    for id_, values in all_data.items():
        data = values["data"]
        values["minhash"] = []
        fingerprint = hasher.fingerprint(data)
        for i in range(bands):
            hash_value = hash(tuple(fingerprint[i * bands_length:(i + 1) * bands_length]))
            values["minhash"].append(hash_value)
            hash_bin[i][hash_value].add(id_)
    for hash_dict in hash_bin:
        for hash_value, id_list in hash_dict.items():
            candidate_pairs.update(set(itertools.combinations(sorted(list(id_list)), r=2)))
    return candidate_pairs


def shingles(text, char_ngram=5):
    return set(text[head:head + char_ngram] for head in range(0, len(text) - char_ngram))


def get_jaccard(set_a, set_b):
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union)


def get_jaccard_all_candidate_pairs(candidate_pairs, data,
                                    jac_thred, jac_baike_thred,
                                    dup_id_set):
    baike_keywords = ['baike', 'wiki']
    for id_a, id_b in candidate_pairs:
        jaccard_sim = get_jaccard(shingles(data[id_a]["data"]), shingles(data[id_b]["data"]))
        if any(keyword in data[id_a]['url'] for keyword in baike_keywords) \
                and any(keyword in data[id_b]['url'] for keyword in baike_keywords):
            thred = jac_baike_thred
        else:
            thred = jac_thred
        if jaccard_sim > thred:
            dup_id_set.add(id_b)
    return dup_id_set


# def save_data():
#     """
#     TODO
#     对重复度较高的数据仅保留一份
#     :return:
#     """
#     return


def get_all_data(data_file_list):
    """
    遍历文件夹
    :param data_file_list:
    :return: {
              id_global:{'path':path, 'id':id, 'url': url, 'data': data},
              id_global:{'path':path, 'id':id, 'url': url, 'data': data},
              ...
              }
    """
    data = {}
    id_global = 0
    for file in data_file_list:
        with open(file, 'r') as r:
            lines = json.load(r)
            for id_, values in lines.items:
                data[id_global]["path"] = file
                data[id_global]["id"] = id_
                data[id_global]["url"] = values["url"]
                data[id_global]["data"] = values["data"]
                id_global += 1
    return data


def insert_to_db_all(data, to_insert_set, dup_set):
    to_insert_db_id_set = copy.deepcopy(to_insert_set)
    dup_id_set = copy.deepcopy(dup_set)
    for id in to_insert_db_id_set:
        to_insert_data = data[id]
        minhash = to_insert_data["minhash"]
        for i, i_minhash in enumerate(minhash):




    return to_insert_db_id_set, dup_id_set


def write_data(data, to_insert_set, dup_set, to_de_dup_path, no_dup_path, dup_path):
    """
    todo 整理格式
    :param data:
    :param to_insert_set:
    :param dup_set:
    :param to_de_dup_path:
    :param no_dup_path:
    :param dup_path:
    :return:
    """
    pass


def de_dup_pipeline(to_de_dup_data_path_list, to_de_dup_path, no_dup_path, dup_path, char_ngram, seeds, bands,
                    hashbytes, jac_thred, jac_baike_thred):
    data = get_all_data(to_de_dup_data_path_list)
    candidate_pairs = get_candidate_pairs(data, char_ngram, seeds, bands, hashbytes)
    to_insert_db_id_set = set()  # 暂时为全部数据，待去数据库查重
    for i in range(len(data)):
        to_insert_db_id_set.add(i)
    # candidate_pairs中hash重复，计算jaccrad后数据分到to_insert_db_id_list或dup_id_list
    dup_id_set = set()  # 暂时为空,jaccard重复
    dup_id_set = get_jaccard_all_candidate_pairs(candidate_pairs, data,
                                                 jac_thred, jac_baike_thred,
                                                 dup_id_set)
    to_insert_db_id_set = to_insert_db_id_set - dup_id_set
    to_insert_db_id_set, dup_id_set = insert_to_db_all(data, to_insert_db_id_set, dup_id_set)
    write_data(data, to_insert_db_id_set, dup_id_set, to_de_dup_path, no_dup_path, dup_path)
    return
