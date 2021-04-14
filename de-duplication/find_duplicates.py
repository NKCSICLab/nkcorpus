import itertools
import json
import time
import sys
from lsh import cache, minhash # https://github.com/mattilyra/lsh

# a pure python shingling function that will be used in comparing
# LSH to true Jaccard similarities
def shingles(text, char_ngram=5):
    return set(text[head:head + char_ngram] for head in range(0, len(text) - char_ngram))


def jaccard(set_a, set_b):
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union)


def candidate_duplicates(document_feed, char_ngram=5, seeds=100, bands=5, hashbytes=4):
    char_ngram = char_ngram
    sims = []
    hasher = minhash.MinHasher(seeds=seeds, char_ngram=char_ngram, hashbytes=hashbytes)
    if seeds % bands != 0:
        raise ValueError('Seeds has to be a multiple of bands. {} % {} != 0'.format(seeds, bands))

    lshcache = cache.Cache(num_bands=bands, hasher=hasher)
    for i_line, line in enumerate(document_feed):
        line = line.decode('utf8')
        myline = json.loads(line)
        mytext = myline['text']
        lineid = myline['id']
        fingerprint = hasher.fingerprint(mytext.encode('utf8'))

        # in addition to storing the fingerpring store the line
        # number and document ID to help analysis later on
        lshcache.add_fingerprint(fingerprint, doc_id=(i_line, lineid))

    candidate_pairs = set()
    for b in lshcache.bins:
        for bucket_id in b:
            if len(b[bucket_id]) > 1:
                pairs_ = set(itertools.combinations(b[bucket_id], r=2))
                candidate_pairs.update(pairs_)

    return candidate_pairs
if __name__ == '__main__':

    starttime=time.time()
    print('finding possible duplicate content ...')
    input = sys.argv[1]
    output = sys.argv[2]
    start_time = time.time()
    #读取输入输出的文件名
    hasher = minhash.MinHasher(seeds=50, char_ngram=5, hashbytes=4)
    lshcache = cache.Cache(bands=5, hasher=hasher)
    lines=[]
    # read in the data file and add the first 100 documents to the LSH cache
    fh = open(input, 'rb')
    for line in fh:
        lines.append(line.decode('utf8'))
    fp = open(output, 'w')

    # reset file pointer and do LSH
    fh.seek(0)
    feed = fh
    candidates = candidate_duplicates(feed, char_ngram=5, seeds=50, bands=5, hashbytes=4)

    # go over all the generated candidates comparing their similarities
    similarities = []
    for ((line_a, docid_a), (line_b, docid_b)) in candidates:
        doc_a, doc_b = lines[line_a], lines[line_b]
        shingles_a = shingles(lines[line_a])
        shingles_b = shingles(lines[line_b])

        jaccard_sim = jaccard(shingles_a, shingles_b)
        fingerprint_a = set(hasher.fingerprint(doc_a.encode('utf8')))
        fingerprint_b = set(hasher.fingerprint(doc_b.encode('utf8')))
        minhash_sim = len(fingerprint_a & fingerprint_b) / len(fingerprint_a | fingerprint_b)
        similarities.append((docid_a, docid_b, jaccard_sim, minhash_sim))

    print('There are {} candidate duplicates in total'.format(len(candidates)))
    endtime=time.time()
    print('总共的时间为:', round(endtime - starttime, 2),'secs')
    for i in similarities:
        if i[2]>0.95:
            #fp.write(str(i[0])+" "+str(i[1])+" "+str(i[2])+" "+str(i[3])+'\n')
            #fp.write(str(i[0]+'\n'))
            fp.write(str(i[1]+'\n'))
