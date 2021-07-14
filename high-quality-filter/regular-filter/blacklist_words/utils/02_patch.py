import shutil

IN_FILE = f'../data/tmp/prefix.txt'
OUT_FILE = f'../data/tmp/chs.txt'
LATEST_FILE = f'../data/latest.txt'

shutil.copy(LATEST_FILE, OUT_FILE)
with open(OUT_FILE, 'a', encoding='utf-8') as out_file, open(IN_FILE, 'r', encoding='utf8') as in_file:
    content = in_file.read()
    out_file.write('\n')
    out_file.write(content)
