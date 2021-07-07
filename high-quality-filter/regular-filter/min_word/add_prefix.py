PREFIX = '色情'
IN_FILE = 'in_file.txt'
OUT_FILE = 'out_file.txt'

with open(IN_FILE, 'r', encoding='utf-8') as new_file, open(OUT_FILE, 'w', encoding='utf-8') as out_file:
    lines = new_file.readlines()
    for line in lines:
        out_file.write(f'{PREFIX} {line}')