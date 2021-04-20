import itertools
import json
import time
import sys

def shingles(text, char_ngram=5):
    return set(text[head:head + char_ngram] for head in range(0, len(text) - char_ngram))


def jaccard(set_a, set_b):
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union)

if __name__ == '__main__':
    inputnum = sys.argv[1]
    inputfile = sys.argv[2]
    outputnum = sys.argv[3]
    jaccardmin = float(sys.argv[4])
    start_time = time.time()
    fh = open(inputnum, 'rb')
    ft = open(inputfile, 'rb')
    fp = open(outputnum, 'w')
    fh.seek(0)
    idlist=[]
    pairlist=[]
    dellist=[]
    textlist=dict()
    for line in fh:
        try:
            a,b=map(int,line.split())
            idlist.append(a)
            idlist.append(b)
            pairlist.append((a,b))
        except:
            break
    idlist.sort()
    #print(idlist)
    for line in ft:
        myid=int(json.loads(line.decode('utf8'))['id'])
        #print(myid)
        for i in idlist:
            if i == myid:
                textlist[myid]=json.loads(line.decode('utf8'))['text']
                break
    #print(textlist)
    for i in pairlist:
        shingles_a = shingles(textlist[i[0]])
        shingles_b = shingles(textlist[i[1]])
        jaccard_sim = jaccard(shingles_a, shingles_b)
        if jaccard_sim > jaccardmin:
            dellist.append(i[1])
    #print(dellist)
    dellist.sort()
    for i in dellist:
        fp.write(str(i))
        fp.write('\n')
    fh.close()
    ft.close()
    fp.close()
