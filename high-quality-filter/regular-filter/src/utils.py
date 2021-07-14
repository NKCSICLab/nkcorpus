import copy
import os
import re
import sys
import time
from typing import Iterable, List, Optional, Dict, Tuple

import numpy as np
from flashtext import KeywordProcessor


class ProgBar:
    """
    Displays a progress bar.

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


def load_dirty_table(file):
    table = {}
    dirty_type = set()
    keyword_processor = KeywordProcessor()
    with open(file, "r") as r:
        for line in r:
            line = line.replace('\n', '')
            min_word = line.split(' ', maxsplit=1)
            type_min = min_word[0]
            word_min = min_word[1]
            dirty_type.add(type_min)
            if type_min in table.keys():
                table[type_min].append(word_min)
            else:
                table.update({type_min: [word_min]})
    keyword_processor.add_keywords_from_dict(table)
    return keyword_processor, dirty_type


def filter_dirty(clean, parameter):
    # 若为脏数据，则整条丢弃
    clean_data = copy.deepcopy(clean)
    keyword_processor, dirty_type = load_dirty_table(parameter["file_name"])
    per_dirty = parameter["per_dirty"]
    num_dirty = parameter["num_dirty"]
    for id_, data in clean.items():
        to_deal_data = data["data"]
        key_words_found = keyword_processor.extract_keywords(to_deal_data, span_info=True)
        data_length = len(to_deal_data)
        dirty_length = len(key_words_found)
        dirty_div_type_length = []
        for dirty_type_i in dirty_type:
            if dirty_type_i in per_dirty:
                per_dirty_i = per_dirty.get(dirty_type_i)
            else:
                per_dirty_i = per_dirty.get("other")
            if dirty_type_i in per_dirty:
                num_dirty_i = num_dirty.get(dirty_type_i)
            else:
                num_dirty_i = num_dirty.get("other")
            length = 0
            count = 0
            content = []
            for el in key_words_found:
                if el[0] == dirty_type_i:
                    length += el[2] - el[1]
                    count += 1
                    content.append(to_deal_data[el[1]:el[2]])
            dirty_div_type_length.append({
                "length": length,
                "count": count,
                "per_dirty": per_dirty_i,
                "num_dirty": num_dirty_i
            })
        if dirty_length != 0:
            for i_dirty in dirty_div_type_length:
                if i_dirty["length"] / data_length > i_dirty["per_dirty"] or i_dirty["count"] > i_dirty["num_dirty"]:
                    del clean_data[id_]
                    break
        return clean_data

def filter_length(clean, parameter):
    clean_data = copy.deepcopy(clean)
    min_length = parameter["min_length"]
    for id, data in clean.items():
        if data["data_length"] < min_length:
            del clean_data[id]
    return clean_data


def filter_end(clean, parameter):
    clean_data = copy.deepcopy(clean)
    end_char = parameter["end_char"]
    for id_, data in clean.items():
        content = data["data"]
        flag = 0
        for i in range(data["data_length"] - 1, -1, -1):
            tem_char = content[i]
            if tem_char in end_char:
                flag = 1
                if i == data["data_length"] - 1:
                    break
                else:
                    clean_data[id_]["data"] = content[:i + 1]
                    clean_data[id_]["data_length"] = len(clean_data[id_]["data"])
                    break
        if i == 0 and flag == 0:
            del clean_data[id_]
    return clean_data


def filter_start(clean, parameter):
    clean_data = copy.deepcopy(clean)
    start_char = parameter["start_char"]
    for id_, data in clean.items():
        content = data["data"].split('\n')
        flag = 0
        for i, line in enumerate(content):
            for tem_char in line:
                if tem_char in start_char:
                    flag = 1
                    break
            if flag == 1:
                break
        if i != 0:
            if i == len(content) - 1 and flag == 0:
                del clean_data[id_]
            else:
                clean_data[id_]["data"] = "\n".join(content[i:])
                clean_data[id_]["data_length"] = len(clean_data[id_]["data"])
    return clean_data


def filter_error_code(clean, parameter):
    clean_data = copy.deepcopy(clean)
    err_code = parameter["err_code"]
    for id_, data in clean.items():
        err_content_list = re.findall(f'[{err_code}]+', data["data"])
        if len(err_content_list) != 0:
            after_sub_data = re.sub(f'[{err_code}]+', '', data["data"])
            if len(after_sub_data) == 0:
                del clean_data[id_]
            else:
                clean_data[id_]["data"] = after_sub_data
                clean_data[id_]["data_length"] = len(clean_data[id_]["data"])
    return clean_data


def data_filter(data, filters):
    clean_data = data
    progbar = ProgBar(len(filters))
    for i in filters:
        clean_data = i["func"](clean_data, i["parameter"])
        progbar.add(1)
    return clean_data


def find_filters(filters):
    all_filter_fuc = {
        "filter_dirty": {"func": filter_dirty, "parameter": None},
        "filter_end": {"func": filter_end, "parameter": None},
        "filter_start": {"func": filter_start, "parameter": None},
        "filter_length": {"func": filter_length, "parameter": None},
        "filter_error_code": {"func": filter_error_code, "parameter": None},
    }

    todo_filter = []
    for i_filter in filters:
        tem_filter = all_filter_fuc.get(i_filter.filter_name)
        tem_filter["parameter"] = eval(i_filter.parameters)
        todo_filter.append(tem_filter)
    return todo_filter


def list_dict_to_dict(data):
    data_dict = {}
    for id_, i in enumerate(data):
        data_dict[str(id_)] = i
    return data_dict


def filter_pipeline(data, filters) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
    """
    :return: {
              id:{'url': url, 'date': date, 'content_length': content_length, 'data': data, 'data_length': data_length},
              id:{'url': url, 'date': date, 'content_length': content_length, 'data': data, 'data_length': data_length},
              ...
              },
              {
              id:{'url': url, 'date': date, 'content_length': content_length, 'data': data, 'data_length': data_length},
              id:{'url': url, 'date': date, 'content_length': content_length, 'data': data, 'data_length': data_length},
              ...
              }
    """
    todo_filter = find_filters(filters)
    data = list_dict_to_dict(data)
    clean_data = data_filter(data, todo_filter)
    return clean_data
