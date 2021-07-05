IN_FILE_CHS = 'min_word_v2_chs.txt'
IN_FILE_CHT = 'min_word_v2_cht.txt'
OUT_FILE = 'min_word_v2_unordered.txt'

out_lines = []
dedup = set()

with open(IN_FILE_CHS, 'r', encoding='utf-8') as chs, open(IN_FILE_CHT, 'r', encoding='utf-8') as cht:
    chs_lines, cht_lines = chs.readlines(), cht.readlines()
    for i in range(len(chs_lines)):
        value, chs_key = chs_lines[i].strip().split(maxsplit=1)
        _, cht_key = cht_lines[i].strip().split(maxsplit=1)
        if chs_key not in dedup:
            out_lines.append(' '.join([value, chs_key]))
            dedup.add(chs_key)
        if cht_key not in dedup:
            out_lines.append(' '.join([value, cht_key]))
            dedup.add(cht_key)

with open(OUT_FILE, 'w', encoding='utf-8') as comb:
    for i in range(l := len(out_lines)):
        if i != l - 1:
            comb.write(f'{out_lines[i]}\n')
        else:
            comb.write(f'{out_lines[i]}')
