import json
import time
import sys
import itertools
from lsh import cache, minhash  # https://github.com/mattilyra/lsh
from collections import defaultdict

# a pure python shingling function that will be used in comparing
# LSH to true Jaccard similarities
def shingles(text, char_ngram=5):
    return set(text[head:head + char_ngram] for head in range(0, len(text) - char_ngram))


def jaccard(set_a, set_b):
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union)


def candidate_duplicates(document_feed, char_ngram=5, seeds=50, bands=5, hashbytes=4):
    char_ngram = char_ngram
    sims = []
    hasher = minhash.MinHasher(seeds=seeds, char_ngram=char_ngram, hashbytes=hashbytes)
    if seeds % bands != 0:
        raise ValueError('Seeds has to be a multiple of bands. {} % {} != 0'.format(seeds, bands))
    hashbin=defaultdict(set)
    candidate_pairs = set()
    minstart=0
    while minstart < seeds:
        feed.seek(0)
        for i_line, line in enumerate(document_feed):
            line = line.decode('utf8')
            myline = json.loads(line)
            mytext = myline['data']
            lineid = myline['id']
            if 'fingerprint' in myline:
                fingerprint = myline['fingerprint']
            else:
                fingerprint = hasher.fingerprint(mytext.encode('utf8'))
            bucketid=hash(tuple(fingerprint[minstart:minstart+10]))
            #print(bucketid)
            hashbin[bucketid].add(lineid)
        print("!")
        for bucket_id in hashbin:
            if len(hashbin[bucket_id]) > 1:
                pairs_ = set(itertools.combinations(hashbin[bucket_id], r=2))
                candidate_pairs.update(pairs_)
        minstart = minstart + 10
        hashbin.clear()
        print(len(candidate_pairs))
    return candidate_pairs


if __name__ == '__main__':

    starttime = time.time()
    print('finding possible duplicate content ...')
    inputf = sys.argv[1]
    outputf = sys.argv[2]
    seednum = int(sys.argv[3])
    bandnum = int(sys.argv[4])
    start_time = time.time()
    hasher = minhash.MinHasher(seeds=seednum, char_ngram=5, hashbytes=4)
    lshcache = cache.Cache(bands=bandnum, hasher=hasher)
    fh = open(inputf, 'rb')
    fp = open(outputf, 'w')
    fh.seek(0)
    feed = fh
    candidates = candidate_duplicates(feed, char_ngram=5, seeds=seednum, bands=bandnum, hashbytes=4)
    for (docid_a, docid_b) in candidates:
        fp.write(str(docid_a)+" "+str(docid_b)+'\n')
    fp.close()
    print('There are {} candidate duplicates in total'.format(len(candidates)))
    endtime=time.time()
    print('总共的时间为:', round(endtime - starttime, 2),'secs')