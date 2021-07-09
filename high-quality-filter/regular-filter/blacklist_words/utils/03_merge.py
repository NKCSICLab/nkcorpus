import datetime

today = datetime.date.today().strftime("%m%d")
IN_FILE_CHS = f'../data/tmp/blacklist_words_{today}_chs.txt'
IN_FILE_CHT = f'../data/tmp/blacklist_words_{today}_cht.txt'
OUT_FILE = f'../data/tmp/blacklist_words_{today}_unordered.txt'

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
        if i != l - 1:
            out_file.write(f'{merged[i]}\n')
        else:
            out_file.write(f'{merged[i]}')
