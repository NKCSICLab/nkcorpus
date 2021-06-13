import json
import sys

if __name__ == '__main__':
    dataf = sys.argv[1]
    inputf = sys.argv[2]
    outputf = sys.argv[3]
    dataf = open(dataf, 'r', encoding='utf-8')
    fi = open(inputf, 'r', encoding='utf-8')
    fo = open(outputf, 'w', encoding='utf-8')
    lines = []
    delline = []
    for i in fi:
        delline.append(int(i))
    fi.close()
    delset = set(delline)
    for line in dataf:
        myline=json.loads(line)
        temp = int(myline['id'])
        if temp in delset:
            continue
        fo.write(line)
    fo.close()
