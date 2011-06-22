'''
Created on Jun 22, 2011

@author: kykamath
'''
from library.twitter import TweetFiles

def tweetsFromFile():
    for tweet in TweetFiles.iterateTweetsFromGzip('data/sample.gz'):
        print tweet['user']['screen_name']

if __name__ == '__main__':
    tweetsFromFile()