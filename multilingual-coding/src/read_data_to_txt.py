import argparse
import simdjson as json
import pathlib


def deal_53(files):
    data = []
    for file in files:
        r_data = json.load(open(file, "r"))["data"]
        for d in r_data:
            d = d["paragraphs"][0]["context"]
            data.append(d)
    return data


def deal_52(files):
    data = []
    for file in files:
        r_data = json.load(open(file, "r"))["data"]
        for d in r_data:
            d = d["context"]
            data.append(d)
    return data


def get_data(files, mode):
    func_dict = {53: deal_53, 52: deal_52}
    func = func_dict[mode]
    data = func(files)
    return data


def write_data(file_path, data):
    with open(file_path, "w") as w:
        for d in data:
            w.write(d + "\n")


def main():
    files = list(pathlib.Path(args.raw_data_dir).rglob("*.json"))
    data = get_data(files, args.mode)
    write_data(args.txt_data_path, data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--raw_data_dir",
        default="data/data",
        type=str,
        required=False,
        help="data dir",
    )
    parser.add_argument(
        "--txt_data_path",
        default="data/save/52.txt",
        type=str,
        required=False,
        help="dst json path",
    )
    parser.add_argument(
        "--mode",
        default=52,
        type=int,
        required=False,
        help="dead func",
    )
    args = parser.parse_args()
    main()
