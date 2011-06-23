'''
Created on Jun 22, 2011

@author: kykamath
'''
from settings import twitter_stream_settings
from library.twitter import TweetFiles, getDateTimeObjectFromTweetTimestamp
from classes import Message


def getMessageObjectForTweet(tweet):
    message = Message(tweet['user']['screen_name'], tweet['id'], tweet['text'], getDateTimeObjectFromTweetTimestamp(tweet['created_at']))
    message.setVector(wordToIdMap, twitter_stream_settings.min_phrase_length, twitter_stream_settings.max_phrase_length)
    return message

def tweetsFromFile():
    for tweet in TweetFiles.iterateTweetsFromGzip('data/sample.gz'):
        print getMessageObjectForTweet(tweet)

if __name__ == '__main__':
    tweetsFromFile()