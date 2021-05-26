import os
import sys
import time
import numpy as np

from typing import Iterable, List, Dict, Optional
from flashtext import KeywordProcessor


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

def load_dirtytable(file_name):
    is_title = 1
    table = {}
    with open(file_name, 'r', encoding='GBK') as lines:
        for line in lines:
            if is_title:
                is_title = 0
                continue
            line = line.replace('\n', '')
            min_word = line.split('\t')
            type_min = min_word[1]
            word_min = min_word[3]
            if type_min in table.keys():
                table[type_min].append(word_min)
            else:
                table.update({type_min:[word_min]})
    keyword_processor.add_keywords_from_dict(table)
    return
keyword_processor = KeywordProcessor()
load_dirtytable('min_word/min_word.txt')
DIRTY_TYPE_0 = '色情'
DIRTY_TYPE_1 = '反动'
DIRTY_TYPE_2 = '暴恐'
DIRTY_TYPE_3 = '民生'
DIRTY_TYPE_4 = '其他'
def filter_dirty(data, per_dirty, per_dirty_sex):
    key_words_found = keyword_processor.extract_keywords(data)
    len_data = len(data)
    len_dirty = len(key_words_found)
    len_dirty_type = [key_words_found.count(DIRTY_TYPE_0),  \
                      key_words_found.count(DIRTY_TYPE_1),  \
                      key_words_found.count(DIRTY_TYPE_2),  \
                      key_words_found.count(DIRTY_TYPE_3),  \
                      key_words_found.count(DIRTY_TYPE_4)]
    
    if(len_dirty!=0 and len_data/len_dirty<PER_DIRTY):
        return ''
    elif(len_dirty_type[0]!=0 and len_data/len_dirty_type[0]<PER_DIRTY_SEX):
        return ''
    else:
        return data

def filter_end(data, endchar):
    check_end = 0
    for i in range(len(data)-1, -1, -1):
        s = data[i]
        if(s in end_char):
           return data[0:i+1]
    return ''

def filter_complete(data, ch_char):
    data = data.split('\n')
    result = ''
    for line in data:
        for line_char in line:
            if(line_char in ch_char):
                line+='\n'
                result+=line
                break
    return result

def filter_length(data, len):
    if(len(data)<len):
        return ''
    else:
        return data

hash_table=set()
def filter_repeat(data):
    data = data.split('\n')
    del data[-1]
    hash_result = ''
    for line in data:
        hash = hashlib.md5()
        hash.update(bytes(line,encoding='utf-8'))
        i = hash.hexdigest()
        if i in a:
            continue
        else:
            line+='\n'
            hash_result+=line
            hash_table.add(i)
    return hash_result
           

def filter_pipline(data, filter):
    #由filter中名字和参数选取过滤器