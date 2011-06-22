'''
Created on Jun 12, 2011

@author: kykamath
'''

#from distributed_map import DistributedMongoMap
#
#DistributedMongoMap().add()
#import multiprocessing
#from multiprocessing import Pool
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


#def iterator():
#    for i in range(100): yield i
#
#def f(x):
#    print x, x**2
#
#pool = Pool()
#print pool.map(f, iterator())

from library.twitter import TweetFiles

for tweets in TweetFiles.iterateTweetsFromGzip('data/sample.gz'):
    print tweets['user']['screen_name']
