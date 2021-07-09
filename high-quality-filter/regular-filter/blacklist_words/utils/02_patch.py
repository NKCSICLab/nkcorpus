import datetime
import pathlib
import shutil

today = datetime.date.today().strftime("%m%d")
IN_FILE = f'../data/tmp/blacklist_words_{today}_prefix.txt'
OUT_FILE = f'../data/tmp/blacklist_words_{today}_chs.txt'

latest_file = sorted(list(pathlib.Path('../data').glob('blacklist_words_*.txt')))[-1]
shutil.copy(str(latest_file), OUT_FILE)
with open(OUT_FILE, 'a', encoding='utf-8') as out_file, open(IN_FILE, 'r', encoding='utf8') as in_file:
    content = in_file.read()
    out_file.write('\n')
    out_file.write(content)
