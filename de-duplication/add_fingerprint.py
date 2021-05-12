import itertools
import json
import time
import sys
from lsh import cache, minhash  # https://github.com/mattilyra/lsh

def shingles(text, char_ngram=5):
    return set(text[head:head + char_ngram] for head in range(0, len(text) - char_ngram))

def add_fingerprints(inputf, outputf, char_ngram=5, seeds=50, bands=5, hashbytes=4):
    char_ngram = char_ngram
    hasher = minhash.MinHasher(seeds=seeds, char_ngram=char_ngram, hashbytes=hashbytes,random_state=7)
    if seeds % bands != 0:
        raise ValueError('Seeds has to be a multiple of bands. {} % {} != 0'.format(seeds, bands))
    lshcache = cache.Cache(num_bands=bands, hasher=hasher)
    line = inputf.readline()
    while line:
        line = line.decode('utf8')
        myline = json.loads(line)
        if 'fingerprint' in myline:
            continue
        mytext = myline['data']
        fingerprint = hasher.fingerprint(mytext.encode('utf8'))
        fingerprint = fingerprint.tolist()
        myline['fingerprint']=fingerprint
        myline = json.dumps(myline)
        fp.write(myline)
        fp.write('\n')
        line = inputf.readline()

if __name__ == '__main__':

    starttime = time.time()
    print('Adding fingerprints...')
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
    add_fingerprints(fh, fp, char_ngram=5, seeds=seednum, bands=bandnum, hashbytes=4)
    fp.close()
    endtime=time.time()
    print('Total time:', round(endtime - starttime, 2),'secs')
