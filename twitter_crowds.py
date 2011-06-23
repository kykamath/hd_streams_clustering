'''
Created on Jun 22, 2011

@author: kykamath
'''
from settings import twitter_stream_settings
from library.twitter import TweetFiles, getDateTimeObjectFromTweetTimestamp
from classes import Message, UtilityMethods, Phrase
import pprint

class TwitterCrowdsSpecificMethods:
    @staticmethod
    def getMessageObjectForTweet(tweet, phraseToIdMap, max_dimensions, min_phrase_length, max_phrase_length):
        message = Message(tweet['user']['screen_name'], tweet['id'], tweet['text'], getDateTimeObjectFromTweetTimestamp(tweet['created_at']))
        message.vector = UtilityMethods.getVectorForText(tweet['text'], phraseToIdMap, max_dimensions, min_phrase_length, max_phrase_length)
        return message

phraseToIdMap = {}

def tweetsFromFile():
    for tweet in TweetFiles.iterateTweetsFromGzip('data/sample.gz'):
        print TwitterCrowdsSpecificMethods.getMessageObjectForTweet(tweet, phraseTextToIdMap=phraseTextToIdMap, 
                                                                    max_dimensions=twitter_stream_settings.max_dimensions,
                                                                    min_phrase_length=twitter_stream_settings.min_phrase_length,
                                                                    max_phrase_length=twitter_stream_settings.max_phrase_length
                                                                    ).vector
    print len(phraseToIdMap)
    print pprint.pprint(phraseToIdMap)
    
if __name__ == '__main__':
    tweetsFromFile()
    ph = Phrase('asd', d, score)