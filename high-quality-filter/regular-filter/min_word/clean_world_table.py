out_lines = []
dedup = set()

with open('min_word_chs.txt', 'r') as chs, open('min_word_cht.txt', 'r') as cht:
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

with open('min_word.txt', 'w') as comb:
    for i in out_lines:
        comb.write(f'{i}\n')
