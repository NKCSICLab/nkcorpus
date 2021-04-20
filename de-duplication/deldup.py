import json
import sys
#需要保证：data文件中，id是从小到大排序的
if __name__ == '__main__':
    dataf = sys.argv[1]
    inputf = sys.argv[2]
    outputf = sys.argv[3]
    dataf = open(dataf, 'r', encoding='utf-8')
    fi = open(inputf, 'r', encoding='utf-8')
    fo = open(outputf, 'w', encoding='utf-8')
    lines=[]
    delline=[]
    for i in fi:
        delline.append(int(i))
    fi.close()
    delset = set(delline)
    delline = list(delset)
    delline.sort()
    delnum=len(delline)
    label=0
    for line in dataf:
        myline=json.loads(line)
        if label!=delnum:
            temp = int(myline['id'])
            if temp==delline[label]:
                label+=1
                continue
            else:
                lines.append(line)
        else:
            lines.append(line)
    for line in lines:
        fo.write(line)
    fo.close()
