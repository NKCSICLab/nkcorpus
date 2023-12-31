import datetime
import shutil

today = datetime.date.today().strftime("%y%m%d")
IN_FILE = f'../data/tmp/unsorted.txt'
OUT_FILE = f'../data/{today}.txt'
LATEST_FILE = '../data/latest.txt'

category_list = []
content_list = []

with open(IN_FILE, 'r', encoding='utf-8') as in_file:
    for line in in_file.readlines():
        category, content = line.strip().split(maxsplit=1)
        if category not in category_list:
            category_list.append(category)
            content_list.append([])
        i = category_list.index(category)
        content_list[i].append(content)
    for content in content_list:
        content.sort()

with open(OUT_FILE, 'w', encoding='utf-8') as out_file:
    for i in range(li := len(category_list)):
        for j in range(lj := len(content_list[i])):
            out_file.write(f'{category_list[i]} {content_list[i][j]}')
            if i != li - 1 or j != lj - 1:
                out_file.write('\n')

shutil.copy(OUT_FILE, LATEST_FILE)
