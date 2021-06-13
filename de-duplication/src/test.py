from lsh import minhash

for _ in range(5):
    hasher = minhash.MinHasher(seeds=100, char_ngram=5,random_state=7)
    fingerprint0 = hasher.fingerprint('Lorem Ipsum dolor sit amet'.encode('utf8'))
    fingerprint1 = hasher.fingerprint('Lorem Ipsum dolor sit amet is how dummy text starts'.encode('utf8'))
    print(len(fingerprint1))
    print(sum(fingerprint0[i] in fingerprint1 for i in range(hasher.num_seeds)) / hasher.num_seeds)