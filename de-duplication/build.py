import transformers
import os
import json
import random
import numpy as np
import argparse
from datetime import datetime
from tqdm import tqdm
from tokenizations import tokenization_bert


def build_files(data_path, tokenized_data_path, num_pieces, full_tokenizer, min_length,input_len):
    #input_len=input_len+2
    reault_train=[]
    reault_label=[]
    with open(data_path, 'r', encoding='utf8') as f:
        print('reading lines')
        lines = json.load(f)
        #lines = [line.replace('\n', '[SEP]') for line in lines]  # 用[SEP]表示换行, 段落之间使用SEP表示段落结束
        lines = [line.replace('\n', '') for line in lines] # 去掉换行
    all_len = len(lines)
    if not os.path.exists(tokenized_data_path):
        os.mkdir(tokenized_data_path)
    for i in tqdm(range(num_pieces)):
        sublines = lines[all_len // num_pieces * i: all_len // num_pieces * (i + 1)]
        if i == num_pieces - 1:
            sublines.extend(lines[all_len // num_pieces * (i + 1):])  # 把尾部例子添加到最后一个piece
        sublines = [full_tokenizer.tokenize(line) for line in sublines if
                    len(line) > min_length]  # 只考虑长度超过min_length的句子
        sublines = [full_tokenizer.convert_tokens_to_ids(line) for line in sublines]
        full_line = []
        for subline in sublines:
            full_line.append(full_tokenizer.convert_tokens_to_ids('[MASK]'))  # 文章开头添加MASK表示文章开始
            full_line.extend(subline)
            full_line.append(full_tokenizer.convert_tokens_to_ids('[CLS]'))  # 文章之间添加CLS表示文章结束
        n = len(full_line)
        if n >= input_len:
            full_line = full_line[:input_len + 1] + full_line[n - 1:]
        # else:
        #     for i in range(input_len-n):
        #         full_line.append(0)
        # for q in range(len(full_line)):
        #     full_line[q] = full_line[q] + 1
        n = len(full_line)
        full_line_data = full_line[:n - 2] + full_line[n - 1:]
        full_line_label = full_line[0:1] + full_line[2:]
        # full_line_data = np.asarray(full_line_data)
        # full_line_label = np.asarray(full_line_label)
        reault_train.append(full_line_data)
        reault_label.append(full_line_label)
    reault_train = np.asarray(reault_train)
    reault_label = np.asarray(reault_label)
    np.save(tokenized_data_path + 'tokenized_train.npy', reault_train)
    np.save(tokenized_data_path + 'tokenized_label.npy', reault_label)
    print('finish')

def main(): # 暂时只考虑输入大于等于窗口的情况
    parser = argparse.ArgumentParser()
    parser.add_argument('--raw_data_path', default='data/train.json', type=str, required=False, help='原始训练语料')
    parser.add_argument('--tokenized_data_path', default='data/tokenized/', type=str, required=False,
                        help='tokenized语料存放位置')
    parser.add_argument('--num_pieces', default=1, type=int, required=False, help='将训练语料分成多少份')
    parser.add_argument('--min_length', default=1, type=int, required=False, help='最短收录文章长度')
    parser.add_argument('--tokenizer_path', default='cache/vocab_small.txt', type=str, required=False, help='选择词库')
    parser.add_argument('--input_len', default=10 , type=int, required=False, help='窗口大小')
    args = parser.parse_args()
    print('args:\n' + args.__repr__())
    raw_data_path = args.raw_data_path
    tokenized_data_path = args.tokenized_data_path
    num_pieces = args.num_pieces
    min_length = args.min_length
    input_len=args.input_len
    full_tokenizer = tokenization_bert.BertTokenizer(vocab_file=args.tokenizer_path)
    #full_tokenizer.max_len = 999999
    print('building files')
    build_files(data_path=raw_data_path, tokenized_data_path=tokenized_data_path, num_pieces=num_pieces,
            full_tokenizer=full_tokenizer, min_length=min_length,input_len=input_len)
    print('files built')

if __name__ == '__main__':
    main()