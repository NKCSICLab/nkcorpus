import os
import re
import sys
import copy
import time
import numpy as np
from typing import Iterable, Sequence, List, Dict, Optional, Tuple, Set

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


def kernel_filter_blacklist_words(clean, deleted, parameters):
    clean_data = copy.deepcopy(clean)
    deleted_data = copy.deepcopy(deleted)
    keyword_processor, categories = load_blacklist_words(parameters['file_name'])
    percentage_limit = parameters['per_dirty']
    count_limit = parameters['num_dirty']
    for id_, data in clean.items():
        unprocessed_data = data['data']
        keywords_found = keyword_processor.extract_keywords(unprocessed_data, span_info=True)
        data_length = len(unprocessed_data)
        category_stats = []
        if len(keywords_found) != 0:
            for category in categories:
                if category in percentage_limit:
                    category_percentage_limit = percentage_limit.get(category)
                else:
                    category_percentage_limit = percentage_limit.get('other')
                if category in count_limit:
                    category_count_limit = count_limit.get(category)
                else:
                    category_count_limit = count_limit.get('other')
                length = 0
                count = 0
                for keyword in keywords_found:
                    if keyword[0] == category:
                        length += keyword[2] - keyword[1]
                        count += 1
                category_stats.append({
                    'length': length,
                    'count': count,
                    'percentage_limit': category_percentage_limit,
                    'count_limit': category_count_limit
                })
            for category_stat in category_stats:
                if category_stat['length'] / data_length > category_stat['percentage_limit'] \
                        or category_stat['count'] > category_stat['count_limit']:
                    deleted_data[id_] = data
                    del clean_data[id_]
                    break
    return clean_data, deleted_data


def kernel_filter_length(clean, deleted, parameter):
    clean_data = copy.deepcopy(clean)
    deleted_data = copy.deepcopy(deleted)
    min_length = parameter["min_length"]
    for id, data in clean.items():
        if data["data_length"] < min_length:
            deleted_data[id] = data
            del clean_data[id]
    return clean_data, deleted_data


def kernel_filter_end(clean, deleted, parameter):
    clean_data = copy.deepcopy(clean)
    deleted_data = copy.deepcopy(deleted)
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
                    if id_ in deleted_data:
                        deleted_data[id_]["data"] = content[i + 1:] + deleted_data[id_]["data"]
                        deleted_data[id_]["data_length"] = len(deleted_data[id_]["data"])
                    else:
                        deleted_data[id_] = data
                        deleted_data[id_]["data"] = content[i + 1:]
                        deleted_data[id_]["data_length"] = len(deleted_data[id_]["data"])
                    break
        if i == 0 and flag == 0:
            deleted_data[id_] = data
            del clean_data[id_]
    return clean_data, deleted_data


def kernel_filter_start(clean, deleted, parameter):
    clean_data = copy.deepcopy(clean)
    deleted_data = copy.deepcopy(deleted)
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
                deleted_data[id_] = data
                del clean_data[id_]
            else:
                clean_data[id_]["data"] = "\n".join(content[i:])
                clean_data[id_]["data_length"] = len(clean_data[id_]["data"])
                if id_ in deleted_data:
                    deleted_data[id_]["data"] = "\n".join(content[:i]) + deleted_data[id_]["data"]
                    deleted_data[id_]["data_length"] = len(deleted_data[id_]["data"])
                else:
                    deleted_data[id_] = data
                    deleted_data[id_]["data"] = "\n".join(content[:i])
                    deleted_data[id_]["data_length"] = len(deleted_data[id_]["data"])
    return clean_data, deleted_data


def kernel_filter_ctrl_char(clean, deleted, parameter):
    clean_data = copy.deepcopy(clean)
    deleted_data = copy.deepcopy(deleted)
    err_code = parameter["err_code"]
    for id_, data in clean.items():
        err_content_list = re.findall(f'[{err_code}]+', data["data"])
        if len(err_content_list) != 0:
            after_sub_data = re.sub(f'[{err_code}]+', '', data["data"])
            if len(after_sub_data) == 0:
                deleted_data[id_] = data
                del clean_data[id_]
            else:
                clean_data[id_]["data"] = after_sub_data
                clean_data[id_]["data_length"] = len(clean_data[id_]["data"])
                if id_ in deleted_data:
                    deleted_data[id_]["data"] = "".join(err_content_list) + deleted_data[id_]["data"]
                    deleted_data[id_]["data_length"] = len(deleted_data[id_]["data"])
                else:
                    deleted_data[id_] = data
                    deleted_data[id_]["data"] = "".join(err_content_list)
                    deleted_data[id_]["data_length"] = len(deleted_data[id_]["data"])
    return clean_data, deleted_data


def load_blacklist_words(path: str) -> Tuple[KeywordProcessor, Set]:
    blacklist_words = {}
    categories = set()
    keyword_processor = KeywordProcessor()
    with open(path, 'r') as file:
        for line in file.readlines():
            line = line.strip()
            if line:
                category, word = line.split(maxsplit=1)
                categories.add(category)
                if category in blacklist_words.keys():
                    blacklist_words[category].append(word)
                else:
                    blacklist_words.update({category: [word]})
    keyword_processor.add_keywords_from_dict(blacklist_words)
    return keyword_processor, categories


def get_filters(filters: Sequence) -> Sequence:
    kernels = {
        'filter_blacklist_words': kernel_filter_blacklist_words,
        'filter_end': kernel_filter_end,
        'filter_start': kernel_filter_start,
        'filter_ctrl_char': kernel_filter_ctrl_char,
        'filter_length': kernel_filter_length,
    }

    selected_filters = []
    for filter_ in filters:
        kernel = kernels.get(filter_.filter_name)
        selected_filters.append({
            'kernel': kernel,
            'parameters': eval(filter_.parameters)
        })
    return selected_filters


def list_to_indexed_dict(data: Sequence) -> Dict:
    data_dict = {}
    for id_, d in enumerate(data):
        data_dict[str(id_)] = d
    return data_dict


def filter_data(data: Sequence, filters: Sequence) -> Tuple:
    """
    :return:
    (
        {
            id: {
                'url': url,
                'date': date,
                'content_length': content_length,
                'data': data,
                'data_length': data_length
            },
            ...
        },
        {
            id: {
                'url': url,
                'date': date,
                'content_length': content_length,
                'data': data,
                'data_length': data_length
            },
            ...
        }
    )
    """
    filters = get_filters(filters)
    data = list_to_indexed_dict(data)
    clean_data = data
    deleted_data = {}
    progbar = ProgBar(len(filters))
    for filter_ in filters:
        clean_data, deleted_data = filter_['kernel'](clean_data, deleted_data, filter_['parameters'])
        progbar.add(1)
    return clean_data, deleted_data
