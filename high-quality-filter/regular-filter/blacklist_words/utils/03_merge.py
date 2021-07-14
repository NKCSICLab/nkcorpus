import datetime

today = datetime.date.today().strftime("%y%m%d")
IN_FILE_CHS = f'../data/tmp/chs.txt'
IN_FILE_CHT = f'../data/tmp/cht.txt'
OUT_FILE = f'../data/tmp/unsorted.txt'

merged = []
keys = set()

with open(IN_FILE_CHS, 'r', encoding='utf-8') as chs, open(IN_FILE_CHT, 'r', encoding='utf-8') as cht:
    chs_lines, cht_lines = chs.readlines(), cht.readlines()
    for i in range(len(chs_lines)):
        value, chs_key = chs_lines[i].strip().split(maxsplit=1)
        _, cht_key = cht_lines[i].strip().split(maxsplit=1)
        if chs_key not in keys:
            merged.append(' '.join([value, chs_key]))
            keys.add(chs_key)
        if cht_key not in keys:
            merged.append(' '.join([value, cht_key]))
            keys.add(cht_key)

with open(OUT_FILE, 'w', encoding='utf-8') as out_file:
    for i in range(l := len(merged)):
        out_file.write(f'{merged[i]}')
        if i != l - 1:
            out_file.write('\n')
