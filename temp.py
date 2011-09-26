import cjson, gzip
def iterateTweetsFromGzip(file):
    for line in gzip.open(file, 'rb'): 
        try:
            data = cjson.decode(line)
            if 'text' in data: yield data
        except: pass

if __name__ == '__main__':
    for tweet in iterateTweetsFromGzip('/mnt/chevron/kykamath/data/twitter/filter/2011_2_6.gz'): print tweet