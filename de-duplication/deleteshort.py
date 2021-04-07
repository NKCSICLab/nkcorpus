import json
import sys
if __name__ == '__main__':
    inputf = sys.argv[1]
    outputf = sys.argv[2]
    fi = open(inputf, 'r', encoding='utf-8')
    fo = open(outputf, 'w', encoding='utf-8')
    lines=[]
    for line in fi:
        myline=json.loads(line)
        mytext=myline['text']
        #print(mytext)
        #print(len(mytext))
        if len(mytext)>5:
            lines.append(line)
    fi.close()
    for l in lines:
        fo.write(l)
    fo.close()


        