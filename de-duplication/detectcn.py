# coding=utf-8
# Copyright (c) 2020, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import json
from langdetect import detect
import time
import sys


def print_progress(prefix, start_time, num_docs,num_fixed_text,
                   num_non_chinese_docs, chars_non_chinese_docs):

    string = prefix + ' | '
    string += 'elapsed time: {:.2f} | '.format(time.time() - start_time)
    string += 'documents: {} | '.format(num_docs)
    string += 'fixed text: {} | '.format(num_fixed_text)
    string += 'non-chinese: {} | '.format(num_non_chinese_docs)
    string += 'non-chinese chars: {} | '.format(chars_non_chinese_docs)
    print(string, flush=True)


def filter_corpus(filename, out_filename, print_interval=10000):

    print(' > filtering {}'.format(filename))
    num_docs = 0
    num_written_docs = 0
    num_fixed_text = 0
    num_non_chinese_docs = 0
    chars_non_chinese_docs = 0
    start_time = time.time()
    with open(out_filename, 'wb') as f:
        with open(filename, 'r') as fin:
            for line in fin:
                try:
                    num_docs += 1
                    myjson = json.loads(line)
                    if detect(myjson['text']) != 'zh-cn':
                        num_non_chinese_docs += 1
                        chars_non_chinese_docs += len(myjson)
                        continue
                    myjson = json.dumps(myjson, ensure_ascii=False)
                    f.write(myjson.encode('utf-8'))
                    f.write('\n'.encode('utf-8'))
                    num_written_docs += 1
                    if num_docs % print_interval == 0:
                        print_progress('[PROGRESS]', start_time, num_docs,
                                   num_fixed_text, num_non_chinese_docs,
                                   chars_non_chinese_docs,)
                except Exception as e:
                    e

    print_progress('[FINAL]', start_time, num_docs,
                   num_non_chinese_docs, num_fixed_text,
                   chars_non_chinese_docs,)


if __name__ == '__main__':

    input_filename = sys.argv[1]
    output_filename = sys.argv[2]

    print('will be reading {}'.format(input_filename))
    print('and will write the results to {}'.format(output_filename))

    filter_corpus(input_filename, output_filename)


