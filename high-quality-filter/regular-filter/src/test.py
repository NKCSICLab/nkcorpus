from flashtext import KeywordProcessor


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


#
#
# def filter_dirty(data, keyword_processor, dirty_type):
#     to_deal_data = data
#     key_words_found = keyword_processor.extract_keywords(to_deal_data, span_info=True)
#     # print(key_words_found)
#     data_length = len(to_deal_data)
#     print(data_length)
#     dirty_length = len(key_words_found)
#     dirty_div_type_length = []
#     for dirty_type_i in dirty_type:
#         length = 0
#         count = 0
#         content = []
#         for el in key_words_found:
#             if el[0] == dirty_type_i:
#                 length += el[2] - el[1]
#                 count += 1
#                 content.append(to_deal_data[el[1]:el[2]])
#         dirty_div_type_length.append({
#             "type": dirty_type_i,
#             "length": length,
#             "content": content,
#             "count": count
#         })
#     if dirty_length != 0:
#         for i_dirty in dirty_div_type_length:
#             if i_dirty['type']=="色情":
#                 print(f"type: {i_dirty['type']}\n",
#                       f"length: {i_dirty['length']}\n",
#                       f"per: {i_dirty['length'] / data_length}\n",
#                       f"content: {i_dirty['content']}\n",
#                       f"count: {i_dirty['count']}"
#                       )
#     return
#
#
# keyword_processor, dirty_type = load_dirty_table("../blacklist_words/data/210709.txt")
# data = open("clean-7.13.txt", "r").readlines()
# for i in data:
#     if i!="\n":
#         i_data = i.split(": ")[1][1:-2]
#         print(i_data)
#         filter_dirty(i_data, keyword_processor, dirty_type)
#         input()
import copy


def filter_dirty(clean, deleted, parameter):
    # 若为脏数据，则整条丢弃
    clean_data = copy.deepcopy(clean)
    deleted_data = copy.deepcopy(deleted)
    keyword_processor, dirty_type = load_dirty_table(parameter["file_name"])
    per_dirty = parameter["per_dirty"]
    num_dirty = parameter["num_dirty"]
    for id_, data in clean.items():
        to_deal_data = data["data"]
        print(f"data: {to_deal_data}")
        print(f"data_length: {len(to_deal_data)}")
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
            if dirty_type_i == "色情":
                print(f"type: {dirty_type_i}\n",
                      f"dirty_length: {length}\n",
                      f"dirty_count: {count}\n",
                      f"dirty_per: {length/data_length}\n",
                      f"content: {content}"
                      )
        if dirty_length != 0:
            for i_dirty in dirty_div_type_length:
                if i_dirty["length"] / data_length > i_dirty["per_dirty"] or i_dirty["count"] > i_dirty["num_dirty"]:
                    deleted_data[id_] = data
                    del clean_data[id_]
                    break
        input()
    return clean_data, deleted_data


def list_dict_to_dict(data):
    data_dict = {}
    for id_, i in enumerate(data):
        data_dict[str(id_)] = i
    return data_dict


import json

processed_data = "CC-MAIN-20201101001251-20201101031251-00719.warc.wet.json"
parameter = {"file_name": "../blacklist_words/data/210709.txt", "per_dirty": {"色情": 0.1, "other": 0.3},
             "num_dirty": {"色情": 30, "other": 40}}
with open(processed_data, 'rb') as r:
    data = json.load(r)['data']
    clean_data = list_dict_to_dict(data)
deleted_data = {}
filter_dirty(clean_data, deleted_data, parameter)
for k,v in deleted_data.items():
    print(v["data"])
    input()