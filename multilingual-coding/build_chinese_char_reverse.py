import os
import json
import numpy as np
import argparse
from tqdm import tqdm
import sys

sys.path.append("..")
from tokenizations import tokenization_bert


def build_files(
        data_path,
        tokenized_data_path,
        num_pieces,
        full_tokenizer,
        min_length,
        window_size,
        stride,
):
    with open(data_path, "r", encoding="utf8") as f:
        print("reading lines")
        lines = json.load(f)
    all_len = len(lines)
    if not os.path.exists(tokenized_data_path):
        os.mkdir(tokenized_data_path)

    print("begin")
    for i in tqdm(range(num_pieces)):
        sublines = lines[all_len // num_pieces * i: all_len // num_pieces * (i + 1)]
        if i == num_pieces - 1:
            sublines.extend(lines[all_len // num_pieces * (i + 1):])
        sublines = [
            full_tokenizer.tokenize(line) for line in sublines  # if len(line) > min_length
        ]
        sublines = [full_tokenizer.convert_tokens_to_ids(line)[::-1] for line in sublines]
        full_line = []
        for subline in sublines:
            full_line.append(
                full_tokenizer.convert_tokens_to_ids("[MASK]")
            )  # 文章开头添加MASK表示文章开始
            full_line.extend(subline)
            full_line.append(
                full_tokenizer.convert_tokens_to_ids("[CLS]")
            )  # 文章之间添加CLS表示文章结束
        to_text = full_line[:-1]
        to_label = full_line[1:]
        text = []
        label = []
        for j in range(0, len(to_text), stride):
            if len(to_text[j: j + window_size]) < window_size:
                text.append(
                    np.asarray(
                        to_text[j: j + window_size]
                        + [full_tokenizer.convert_tokens_to_ids("[CLS]")]
                        * (window_size - len(to_text[j: j + window_size]))
                    )
                )
                label.append(
                    np.asarray(
                        to_label[j: j + window_size]
                        + [full_tokenizer.convert_tokens_to_ids("[CLS]")]
                        * (window_size - len(to_text[j: j + window_size]))
                    )
                )
            else:
                text.append(np.asarray(to_text[j: j + window_size]))
                label.append(np.asarray(to_label[j: j + window_size]))
        text = np.asarray(text)
        label = np.asarray(label)
        np.save(tokenized_data_path + "text_{}".format(i), text)
        np.save(tokenized_data_path + "label_{}".format(i), label)
    print("finish")


def main(args):  # 暂时只考虑输入大于等于窗口的情况
    full_tokenizer = tokenization_bert.BertTokenizer(vocab_file=args.tokenizer_path)
    print("building files")
    build_files(
        data_path=args.raw_data_path,
        tokenized_data_path=args.tokenized_data_path,
        num_pieces=args.num_pieces,
        full_tokenizer=full_tokenizer,
        min_length=args.min_length,
        window_size=args.window_size,
        stride=args.stride,
    )
    print("files built")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--raw_data_path",
        default="../../gpt3_dataset/doupo/doupo_all.json",
        type=str,
        required=False,
        help="原始训练语料",
    )
    parser.add_argument(
        "--tokenized_data_path",
        default="../../gpt3_dataset/doupo/doupo_tokenized_reverse/",
        type=str,
        required=False,
        help="tokenized语料存放位置",
    )
    parser.add_argument(
        "--num_pieces", default=1, type=int, required=False, help="将训练语料分成多少份"
    )
    parser.add_argument(
        "--min_length", default=100, type=int, required=False, help="最短收录文章长度"
    )
    parser.add_argument(
        "--tokenizer_path",
        default="cache/vocab_small.txt",
        type=str,
        required=False,
        help="选择词库",
    )
    parser.add_argument(
        "--window_size", default=1024, type=int, required=False, help="窗口大小"
    )
    parser.add_argument(
        "--stride", default=500, type=int, required=False, help="训练时取训练数据的窗口步长"
    )
    args = parser.parse_args()
    main(args)
