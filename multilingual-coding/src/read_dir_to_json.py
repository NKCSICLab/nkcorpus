import argparse
import ast
import simdjson as json
import os
import pathlib


def get_text(file):
    data = []
    with open(file, 'r') as r:
        for line in r:
            content = ast.literal_eval(line)
            data.append(content['text'])
    return data


def walk_deal_all_file(dir, data):
    for _, dirs, files in os.walk(dir):
        for d in dirs:
            walk_deal_all_file(d, data)
        for f in files:
            data.extend(get_text(f))


def get_all_file_data(dir):
    data = []
    p = pathlib.Path(dir)
    for f in p.rglob('*'):
        if f.is_file():
            data.extend(get_text(f))
    return data


def main(args):
    json_data = get_all_file_data(args.raw_data_dir)
    with open(args.json_data_path, 'w') as w:
        json.dump(json_data, w)
    print(len(json_data))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--raw_data_dir",
        default="../../gpt3_dataset/wiki_zh_2019",
        type=str,
        required=False,
        help="data dir",
    )
    parser.add_argument(
        "--json_data_path",
        default="../../gpt3_dataset/wiki_all.json",
        type=str,
        required=False,
        help="dst json path",
    )
    args = parser.parse_args()
    main(args)
