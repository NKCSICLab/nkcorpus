import copy
import itertools
import json
import pathlib
from collections import defaultdict

from lsh import minhash  # https://github.com/mattilyra/lsh
from pymongo.database import Database, Collection


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
    baike_keywords = ['baike', 'wikipedia']
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


def get_all_data(data_file_list):
    """
    遍历文件夹
    :param data_file_list:
    :return:
    """

    """
     {
              id_global:{'path':path, 'id':id, 'url': url, 'data': data},
              id_global:{'path':path, 'id':id, 'url': url, 'data': data},
              ...
              }
    """
    data = {}
    id_global = 0
    for file in sorted(data_file_list):
        with open(file, 'r') as r:
            lines = json.load(r)
            for id_, values in lines.items():
                data[id_global] = values
                # data[id_global]["url"] = values["url"]
                # data[id_global]["data"] = values["data"]
                data[id_global]["path"] = file
                data[id_global]["id"] = id_
                id_global += 1
    return data


def insert_to_db(mongo_db_engine, database, collection, to_insert_data, tag):
    # try:
    db: Database = mongo_db_engine[database]
    cl: Collection = db[collection]
    cl.insert_one({"path": str(to_insert_data["path"]),
                   "id": to_insert_data["id"],
                   "url": to_insert_data["url"],
                   "hash_0": to_insert_data["minhash"][0],
                   "hash_1": to_insert_data["minhash"][1],
                   "hash_2": to_insert_data["minhash"][2],
                   "hash_3": to_insert_data["minhash"][3],
                   "hash_4": to_insert_data["minhash"][4],
                   "tag": tag
                   })


# except Exception as e:
#     print(f"Unable to connect to the mongo database server: {e}.")


def cal_sim_with_db(to_insert, to_check_in_db, jac_thred, jac_baike_thred):
    to_insert_url = to_insert["url"]
    to_insert_data = to_insert["data"]
    baike_keywords = ['baike', 'wikipedia']
    if_insert_baike = any(keyword in to_insert_url for keyword in baike_keywords)
    if not if_insert_baike:
        thred = jac_thred
        for db_rec in to_check_in_db:
            with open(db_rec["path"], "r") as r:
                db_data = json.load(r).get(db_rec["id"])
            jaccard_sim = get_jaccard(shingles(to_insert_data), shingles(db_data))
            if jaccard_sim > thred and (
                    to_check_in_db["id"] != to_insert["id"] or to_check_in_db["path"] != to_insert["path"]):
                return True
    else:
        for db_rec in to_check_in_db:
            with open(db_rec["path"], "r") as r:
                db_data = json.load(r).get(db_rec["id"])
            jaccard_sim = get_jaccard(shingles(to_insert_data), shingles(db_data))
            db_url = db_rec["url"]
            if any(keyword in db_url for keyword in baike_keywords):
                thred = jac_baike_thred
            else:
                thred = jac_thred
            if jaccard_sim > thred:
                return True

    return False


def check_insert_to_db_all(data, to_insert_set, dup_set, mongo_db_engine, database, collection, jac_thred,
                           jac_baike_thred, tag):
    to_insert_db_id_set = copy.deepcopy(to_insert_set)
    dup_id_set = copy.deepcopy(dup_set)
    for id_ in to_insert_set:
        to_insert_data = data[id_]
        minhash = to_insert_data["minhash"]
        to_check_in_db = []
        for i, i_minhash in enumerate(minhash):
            # try:
            db: Database = mongo_db_engine[database]
            cl: Collection = db[collection]
            if cl.count_documents({f"hash_{i}": i_minhash, 'tag': {'lt': tag}}) != 0:
                result = cl.find({f"hash_{i}": i_minhash, 'tag': {'lt': tag}})
                to_check_in_db.extend(list(result))
        # except Exception as e:
        #     print(f"Unable to connect to the mongo database server: {e}.")
        if len(to_check_in_db) == 0:
            insert_to_db(mongo_db_engine, database, collection, to_insert_data, tag)
        else:
            sim = cal_sim_with_db(to_insert_data, to_check_in_db, jac_thred, jac_baike_thred)
            if sim:
                to_insert_db_id_set.remove(id_)
                dup_id_set.add(id_)
            else:
                insert_to_db(mongo_db_engine, database, collection, to_insert_data, tag)
    return to_insert_db_id_set, dup_id_set


def write_div_data(all_data, to_write_set, to_remove_prefix, to_add_prefix):
    path_data = {}
    for global_id_ in to_write_set:
        data = all_data[global_id_]
        path = pathlib.Path(data.pop("path"))
        id_ = data.pop("id")
        to_add_data = {}
        to_add_data[id_] = data
        data = list(data.items())[0]
        to_write_path = pathlib.Path(to_add_prefix).joinpath(path.relative_to(to_remove_prefix)).as_posix()
        if to_write_path in path_data:
            path_data[to_write_path][data[0]] = data[1]
        else:
            path_data[to_write_path] = {}
            path_data[to_write_path][data[0]] = data[1]
    for path, values in path_data.items():
        with open(path, "w") as w:
            json.dump(values, w)
    path_data.clear()


def write_data(all_data, to_insert_set, dup_set, to_de_dup_path, no_dup_path, dup_path):
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
    write_div_data(all_data, to_insert_set, to_de_dup_path, no_dup_path)
    write_div_data(all_data, dup_set, to_de_dup_path, dup_path)
    return


def de_dup_pipeline(to_de_dup_data_path_list, to_de_dup_path, no_dup_path, dup_path, char_ngram, seeds, bands,
                    hashbytes, jac_thred, jac_baike_thred, mongo_db_engine, database, collection):
    import time
    tag = time.time()
    s = time.time()
    data = get_all_data(to_de_dup_data_path_list)
    print(f'get_all_data {time.time() - s}')
    s = time.time()
    candidate_pairs = get_candidate_pairs(data, char_ngram, seeds, bands, hashbytes)
    print(f'get_candidate_pairs {time.time() - s}')
    s = time.time()
    to_insert_db_id_set = set()  # 暂时为全部数据，待去数据库查重
    for i in range(len(data)):
        to_insert_db_id_set.add(i)
    # candidate_pairs中hash重复，计算jaccrad后数据分到to_insert_db_id_list或dup_id_list
    dup_id_set = set()  # 暂时为空,jaccard重复
    dup_id_set = get_jaccard_all_candidate_pairs(candidate_pairs, data,
                                                 jac_thred, jac_baike_thred,
                                                 dup_id_set)
    print(f'get_jaccard_all_candidate_pairs {time.time() - s}')
    s = time.time()
    to_insert_db_id_set = to_insert_db_id_set - dup_id_set
    to_insert_db_id_set, dup_id_set = check_insert_to_db_all(data, to_insert_db_id_set, dup_id_set, mongo_db_engine,
                                                             database, collection, jac_thred, jac_baike_thred, tag)
    print(f'check_insert_to_db_all {time.time() - s}')
    s = time.time()
    write_data(data, to_insert_db_id_set, dup_id_set, to_de_dup_path, no_dup_path, dup_path)
    return
