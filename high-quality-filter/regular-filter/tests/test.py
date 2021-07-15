import json
import copy

from flashtext import KeywordProcessor
from typing import Tuple, Set, Sequence, Dict

PATH = '/mnt/2020-10/original/crawl-data/CC-MAIN-2020-45/segments/1603107874637.23/wet/CC-MAIN-20201021010156-20201021040156-00353.warc.wet.json'
PARAMETERS = {
    'file_name': '../blacklist_words/data/latest.txt',
    'per_dirty': {'色情': 0.1, 'other': 0.3},
    'num_dirty': {'色情': 30, 'other': 40}
}


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

                # BEGIN DEBUG
                content = []
                # END DEBUG

                for keyword in keywords_found:
                    if keyword[0] == category:
                        length += keyword[2] - keyword[1]
                        count += 1

                        # BEGIN DEBUG
                        content.append(unprocessed_data[keyword[1]:keyword[2]])
                        # END DEBUG

                category_stats.append({
                    'length': length,
                    'count': count,
                    'percentage_limit': category_percentage_limit,
                    'count_limit': category_count_limit
                })

                # BEGIN DEBUG
                # print({
                #     'category': category,
                #     'length': length,
                #     'count': count,
                #     'percentage': length / data_length,
                #     'percentage_limit': category_percentage_limit,
                #     'count_limit': category_count_limit,
                #     'content': content
                # })
                # END DEBUG

            for category_stat in category_stats:
                if category_stat['length'] / data_length > category_stat['percentage_limit'] \
                        or category_stat['count'] > category_stat['count_limit']:
                    deleted_data[id_] = data
                    del clean_data[id_]
                    break
    return clean_data, deleted_data


def list_to_indexed_dict(data: Sequence) -> Dict:
    data_dict = {}
    for id_, d in enumerate(data):
        data_dict[str(id_)] = d
    return data_dict


with open(PATH, 'rb') as file:
    data = json.load(file)['data']
    clean_ = list_to_indexed_dict(data)
    deleted_ = {}
    clean_data_, deleted_data_ = kernel_filter_blacklist_words(clean_, deleted_, PARAMETERS)

for k, v in clean_data_.items():
    print(v['data'])
    input()
