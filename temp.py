'''
Created on Jun 12, 2011

@author: kykamath
'''

#from distributed_map import DistributedMongoMap
#
#DistributedMongoMap().add()
#import multiprocessing
from multiprocessing import Pool

from Bio import trie
tr = trie.trie()
tr['10']=100
print tr['10']
del tr['10']
print tr['10']

#
#print multiprocessing.cpu_count()
#
#def f(x):
#    return x*x
#
#if __name__ == '__main__':
#    pool = Pool(processes=4)              # start 4 worker processes
#
#    result = pool.apply_async(f, (10,))    # evaluate "f(10)" asynchronously
#    print result.get(timeout=1)           # prints "100" unless your computer is *very* slow
#
#    print pool.map(f, range(10))          # prints "[0, 1, 4,..., 81]"
#
#    it = pool.imap(f, range(10))
#    print it.next()                       # prints "0"
#    print it.next()                       # prints "1"
#    print it.next(timeout=1)              # prints "4" unless your computer is *very* slow
#
#    import time
#    result = pool.apply_async(time.sleep, (10,))
#    print result.get(timeout=1)           # raises TimeoutError

#d = dict((i,1) for i in range(10))
#
#class ob:
#    def __init__(self,i): self.val=i
#
#def iterator():
#    for i in range(10): yield (d, ob(i))
#
#def f((d, ob)): 
#    if ob.val<5: 
#        print 'comes here', ob.val
#        del d[ob.val] 
#
#pool = Pool()
#print d
#print pool.map(f, iterator())
#print d

#from library.twitter import TweetFiles
#from library.nlp import StopWords, getWordsFromRawEnglishMessage, getPhrases
#
#StopWords.load()
#min_phrase_length = 2
#max_phrase_length = 8
#for tweets in TweetFiles.iterateTweetsFromGzip('data/sample.gz'):
#    print tweets['user']['screen_name'], getPhrases(getWordsFromRawEnglishMessage(tweets['text']), min_phrase_length, max_phrase_length)