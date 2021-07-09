import datetime

today = datetime.date.today().strftime("%m%d")
PREFIX = '色情'
IN_FILE = f'../data/tmp/blacklist_words_{today}_delta.txt'
OUT_FILE = f'../data/tmp/blacklist_words_{today}_prefix.txt'

with open(IN_FILE, 'r', encoding='utf-8') as in_file, open(OUT_FILE, 'w', encoding='utf-8') as out_file:
    lines = in_file.readlines()
    for line in lines:
        out_file.write(f'{PREFIX} {line}')
