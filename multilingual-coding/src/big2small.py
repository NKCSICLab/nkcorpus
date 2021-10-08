import argparse


def main():
    data = open(args.big, "r").readlines()
    lines_num = len(data)
    div_num = lines_num // args.num
    for i in range(args.num):
        if i != args.num - 1:
            i_data = data[i * div_num:(i + 1) * div_num]
        else:
            i_data = data[i * div_num:]
        with open(f"{args.small}{i}.txt", "w") as w:
            for w_data in i_data:
                w.write(w_data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--big",
        default="/home/lidongwen/gpt3-dataset/txt/online/clue_oscar.txt",
        type=str,
        required=False,
        help="data dir",
    )
    parser.add_argument(
        "--small",
        default="/home/lidongwen/gpt3-dataset/txt/online/clue_oscar_",
        type=str,
        required=False,
        help="dst json path",
    )
    parser.add_argument(
        "--num",
        default=30,
        type=int,
        required=False,
        help="dead func",
    )
    args = parser.parse_args()
    main()
