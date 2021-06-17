import copy
import itertools
import json
import pathlib
import sys
import os
import numpy as np
import time
from collections import defaultdict

from lsh import minhash  # https://github.com/mattilyra/lsh
from pymongo.database import Database, Collection
from typing import Optional, List, Iterable


class ProgBar:
    """Displays a progress bar.

      Arguments:
          target: Total number of steps expected, None if unknown.
          width: Progress bar width on screen.
          stateful_metrics: Iterable of string names of metrics that should *not* be
            averaged over time. Metrics in this list will be displayed as-is. All
            others will be averaged by the progbar before display.
          interval: Minimum visual progress update interval (in seconds).
    """

    def __init__(self,
                 target: Optional[int] = None,
                 width: int = 30,
                 interval: float = 0.05,
                 stateful_metrics: Optional[Iterable] = None):
        self._target = target
        self._width = width
        self._interval = interval
        if stateful_metrics:
            self._stateful_metrics = set(stateful_metrics)
        else:
            self._stateful_metrics = set()

        self._dynamic_display = ((hasattr(sys.stdout, 'isatty') and
                                  sys.stdout.isatty()) or
                                 'ipykernel' in sys.modules or
                                 'posix' in sys.modules or
                                 'PYCHARM_HOSTED' in os.environ)
        self._total_width = 0
        self._seen_so_far = 0
        # We use a dict + list to avoid garbage collection issues found in OrderedDict
        self._values = {}
        self._values_order = []
        self._start = time.time()
        self._last_update = 0

    def update(self,
               current: int,
               values: Optional[List] = None,
               finalize: Optional[bool] = None):
        """Updates the progress bar.

        Arguments:
            current: Index of current step.
            values: List of tuples: `(name, value_for_last_step)`. If `name` is in
              `stateful_metrics`, `value_for_last_step` will be displayed as-is.
              Else, an average of the metric over time will be displayed.
            finalize: Whether this is the last update for the progress bar. If
              `None`, defaults to `current >= self._target`.
        """
        if finalize is None:
            if self._target is None:
                finalize = False
            else:
                finalize = current >= self._target

        values = values or []
        for k, v in values:
            if k not in self._values_order:
                self._values_order.append(k)
            if k not in self._stateful_metrics:
                # Force the minimal value to 1 here, otherwise stateful_metric will be 0s.
                value_base = max(current - self._seen_so_far, 1)
                if k not in self._values:
                    self._values[k] = [v * value_base, value_base]
                else:
                    self._values[k][0] += v * value_base
                    self._values[k][1] += value_base
            else:
                # Stateful metrics output a numeric value. This representation
                # means "take an average from a single value" but keeps the
                # numeric formatting.
                self._values[k] = [v, 1]
        self._seen_so_far = current

        now = time.time()
        info = f' - {now - self._start:.0f}s'
        if now - self._last_update < self._interval and not finalize:
            return

        prev_total_width = self._total_width
        if self._dynamic_display:
            sys.stdout.write('\b' * prev_total_width)
            sys.stdout.write('\r')
        else:
            sys.stdout.write('\n')

        if self._target is not None:
            num_digits = int(np.log10(self._target)) + 1
            bar = f'{current:{num_digits}d}/{self._target} ['
            progress = float(current) / self._target
            progress_width = int(self._width * progress)
            if progress_width > 0:
                bar += ('=' * (progress_width - 1))
                if current < self._target:
                    bar += '>'
                else:
                    bar += '='
            bar += ('.' * (self._width - progress_width))
            bar += ']'
        else:
            bar = f'{current:7d}/Unknown'

        self._total_width = len(bar)
        sys.stdout.write(bar)

        if current:
            time_per_unit = (now - self._start) / current
        else:
            time_per_unit = 0

        if self._target is not None and not finalize:
            eta = time_per_unit * (self._target - current)
            if eta > 3600:
                eta_format = f'{eta // 3600:.0f}:{(eta % 3600) // 60:02.0f}:{eta % 60:02.0f}'
            elif eta > 60:
                eta_format = f'{eta // 60:.0f}:{eta % 60:02.0f}'
            else:
                eta_format = f'{eta:.0f}s'
            info = f' - ETA: {eta_format}'

        for k in self._values_order:
            info += f' - {k}:'
            if isinstance(self._values[k], list):
                avg = np.mean(self._values[k][0] / max(1, self._values[k][1]))
                if abs(avg) > 1e-3:
                    info += f' {avg:.4f}'
                else:
                    info += f' {avg:.4e}'
            else:
                info += f' {self._values[k]}'

        self._total_width += len(info)
        if prev_total_width > self._total_width:
            info += (' ' * (prev_total_width - self._total_width))

        if finalize:
            info += '\n'

        sys.stdout.write(info)
        sys.stdout.flush()

        self._last_update = now

    def add(self, n, values=None):
        self.update(self._seen_so_far + n, values)


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
                                    jac_thred, jac_baike_thred):
    dup_id_set = set()
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


def insert_to_db(insert_list, to_insert_data):
    insert_list.append({"path": str(to_insert_data["path"]),
                        "id": to_insert_data["id"],
                        "url": to_insert_data["url"],
                        "hash_0": to_insert_data["minhash"][0],
                        "hash_1": to_insert_data["minhash"][1],
                        "hash_2": to_insert_data["minhash"][2],
                        "hash_3": to_insert_data["minhash"][3],
                        "hash_4": to_insert_data["minhash"][4],
                        })


def cal_sim_with_db(to_insert, to_check_in_db, jac_thred, jac_baike_thred):
    to_insert_url = to_insert["url"]
    to_insert_data = to_insert["data"]
    baike_keywords = ['baike', 'wikipedia']
    to_check_in_db_ = {}
    for db_rec in to_check_in_db:
        path = db_rec["path"]
        if path in to_check_in_db_:
            to_check_in_db_[path].append(db_rec)
        else:
            to_check_in_db_[path] = [db_rec]
    if_insert_baike = any(keyword in to_insert_url for keyword in baike_keywords)
    if not if_insert_baike:
        thred = jac_thred
        for path, db_rec_list in to_check_in_db_.items():
            with open(path, "r") as r:
                file_data = json.load(r)
            print('Woohoo!')
            for db_rec in db_rec_list:
                if db_rec["id"] == to_insert["id"] and str(db_rec["path"]) == str(to_insert["path"]):
                    return True
                db_data = file_data.get(db_rec["id"])
                jaccard_sim = get_jaccard(shingles(to_insert_data), shingles(db_data))
                if jaccard_sim > thred:
                    return True
    else:
        for path, db_rec_list in to_check_in_db_.items():
            with open(path, "r") as r:
                file_data = json.load(r)
            for db_rec in db_rec_list:
                if db_rec["id"] == to_insert["id"] and db_rec["path"] == to_insert["path"]:
                    return True
                db_data = file_data.get(db_rec["id"])
                jaccard_sim = get_jaccard(shingles(to_insert_data), shingles(db_data))
                db_url = db_rec["url"]
                if any(keyword in db_url for keyword in baike_keywords):
                    thred = jac_baike_thred
                else:
                    thred = jac_thred
                if jaccard_sim > thred:
                    return True

        for db_rec in to_check_in_db:
            if db_rec["id"] == to_insert["id"] and db_rec["path"] == to_insert["path"]:
                return True
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
                           jac_baike_thred):
    to_insert_db_id_set = copy.deepcopy(to_insert_set)
    dup_id_set = copy.deepcopy(dup_set)
    """s"""
    import time
    insert_time = 0
    find_db_time = 0
    cal_time = 0
    """e"""
    db: Database = mongo_db_engine[database]
    cl: Collection = db[collection]
    insert_list = []
    progbar = ProgBar(target=len(to_insert_set), stateful_metrics=['count'])
    for id_ in to_insert_set:
        to_insert_data = data[id_]
        minhash = to_insert_data["minhash"]
        """s"""
        s = time.time()
        """e"""

        condition = {"$or": [{f"hash_{i}": i_minhash} for i, i_minhash in enumerate(minhash)]}
        if cl.count_documents(condition) == 0:
            """s"""
            find_db_time += time.time() - s
            """e"""
            """s"""
            s = time.time()
            """e"""
            insert_to_db(insert_list, to_insert_data)
            """s"""
            insert_time += time.time() - s
            """e"""
        else:
            """s"""
            s = time.time()
            """e"""
            to_check_in_db = list(cl.find(condition))
            """s"""
            find_db_time += time.time() - s
            """e"""
            """s"""
            s = time.time()
            """e"""
            sim = cal_sim_with_db(to_insert_data, to_check_in_db, jac_thred, jac_baike_thred)
            """s"""
            cal_time += time.time() - s
            """e"""
            if sim:
                to_insert_db_id_set.remove(id_)
                dup_id_set.add(id_)
            else:
                """s"""
                s = time.time()
                """e"""
                insert_to_db(insert_list, to_insert_data)
                """s"""
                insert_time += time.time() - s
                """e"""
        progbar.add(1, values=[('count', 0)])
    cl.insert_many(insert_list)
    print("*" * 40)
    print(f"{insert_time=}")
    print(f"{find_db_time=}")
    print(f"{cal_time=}")
    print("*" * 40)
    return to_insert_db_id_set, dup_id_set


def write_div_data(all_data, to_write_set, to_remove_prefix, to_add_prefix):
    path_data = {}
    for global_id_ in to_write_set:
        data = all_data[global_id_]
        path = pathlib.Path(data.pop("path"))
        id_ = data.pop("id")
        data.pop("minhash")
        to_write_path = pathlib.Path(to_add_prefix).joinpath(path.relative_to(to_remove_prefix)).as_posix()
        if to_write_path in path_data:
            path_data[to_write_path][id_] = data
        else:
            path_data[to_write_path] = {}
            path_data[to_write_path][id_] = data
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
    dup_id_set = get_jaccard_all_candidate_pairs(candidate_pairs, data,
                                                 jac_thred, jac_baike_thred)
    print(f"dup_id_set {len(dup_id_set)}")
    print(f'get_jaccard_all_candidate_pairs {time.time() - s}')
    s = time.time()
    to_insert_db_id_set = to_insert_db_id_set - dup_id_set
    to_insert_db_id_set, dup_id_set = check_insert_to_db_all(data, to_insert_db_id_set, dup_id_set, mongo_db_engine,
                                                             database, collection, jac_thred, jac_baike_thred)
    print(f"dup_id_set {len(dup_id_set)}")
    print(f'check_insert_to_db_all {time.time() - s}')
    write_data(data, to_insert_db_id_set, dup_id_set, to_de_dup_path, no_dup_path, dup_path)
    return
