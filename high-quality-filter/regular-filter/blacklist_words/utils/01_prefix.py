PREFIX = '色情'
IN_FILE = f'../data/tmp/delta.txt'
OUT_FILE = f'../data/tmp/prefix.txt'

with open(IN_FILE, 'r', encoding='utf-8') as in_file, open(OUT_FILE, 'w', encoding='utf-8') as out_file:
    lines = in_file.readlines()
    striped = []
    for line in lines:
        line = line.strip()
        if line:
            striped.append(line)
    for i in range(l := len(striped)):
        out_file.write(f'{PREFIX} {striped[i]}')
        if i != l - 1:
            out_file.write('\n')
