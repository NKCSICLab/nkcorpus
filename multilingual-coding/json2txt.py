import argparse
import simdjson as json
import os
import sys

import numpy as np
from tqdm import tqdm

sys.path.append("../..")
from tokenizations import tokenization_bert


def json2txt(infile, outfile):
    with open(infile, 'r') as r, open(outfile, 'w') as w:
        for l in r:
            data = json.loads(l)["text"]
            w.write(data.replace("\n", "\\n") + "\n")


def data2id(infile, outfile, tokenizer_path):
    full_tokenizer = tokenization_bert.BertTokenizer(vocab_file=tokenizer_path)
    all_data = []
    with open(infile, 'r') as r:
        for l in r:
            data = json.loads(l)["text"]
            data_tokenized = []
            data_tokenized.append(
                full_tokenizer.convert_tokens_to_ids("[MASK]")
            )  # 文章开头添加MASK表示文章开始
            data_tokenized.extend(full_tokenizer.convert_tokens_to_ids(full_tokenizer.tokenize(data)))
            data_tokenized.append(
                full_tokenizer.convert_tokens_to_ids("[CLS]")
            )  # 文章之间添加CLS表示文章结束
            all_data.extend(data_tokenized)
    np.save(outfile, np.array(all_data))


def split_data(infile, outfile, window, step):
    data = np.load(infile, allow_pickle=True)
    print(f"字数： {data.shape[0]}")
    data_win = []
    for i in range(0, data.shape[0], step):
        if len(data[i:i + window]) == window:
           data_win.append(data[i:i + window])
    data_win = np.array(data_win)
    print(data_win.shape)
    print(f"重复字数： {data_win.shape[0]*data_win.shape[1]-data.shape[0]}")
    np.save(outfile, data_win)


# json2txt("test_data/wiki_00", "test_data/wiki_00.txt")
# data2id("test_data/wiki_00", "test_data/wiki_00_id.npy", "word/vocab_small.txt")
split_data("test_data/wiki_00_id.npy", "test_data/data.npy", 1024, 896)
