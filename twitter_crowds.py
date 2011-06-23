'''
Created on Jun 22, 2011

@author: kykamath
'''
from settings import twitter_stream_settings
from library.twitter import TweetFiles, getDateTimeObjectFromTweetTimestamp
from classes import Message, UtilityMethods

class TwitterCrowdsSpecificMethods:
    @staticmethod
    def getMessageObjectForTweet(tweet, phraseToIdMap, max_dimensions, min_phrase_length, max_phrase_length):
        message = Message(tweet['user']['screen_name'], tweet['id'], tweet['text'], getDateTimeObjectFromTweetTimestamp(tweet['created_at']))
        message.vector = UtilityMethods.getVectorForText(tweet['text'], phraseToIdMap, max_dimensions, min_phrase_length, max_phrase_length)
        return message

wordToIdMap = {}

def tweetsFromFile():
    for tweet in TweetFiles.iterateTweetsFromGzip('data/sample.gz'):
#        print getMessageObjectForTweet(tweet)
        print tweet['created_at']
        exit()

if __name__ == '__main__':
    tweetsFromFile()