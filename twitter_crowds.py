'''
Created on Jun 22, 2011

@author: kykamath
'''
from settings import twitter_stream_settings
from library.twitter import TweetFiles, getDateTimeObjectFromTweetTimestamp
from classes import Message, UtilityMethods, Stream, VectorUpdateMethods
import pprint

class TwitterCrowdsSpecificMethods:
    @staticmethod
    def getMessageObjectForTweet(tweet, phraseTextToIdMap, phraseTextToPhraseObjectMap, **twitter_stream_settings):
        tweetTime = getDateTimeObjectFromTweetTimestamp(tweet['created_at'])
        message = Message(tweet['user']['screen_name'], tweet['id'], tweet['text'], tweetTime)
        message.vector = UtilityMethods.getVectorForText(tweet['text'], tweetTime, phraseTextToIdMap, phraseTextToPhraseObjectMap, **twitter_stream_settings)
        return message

phraseTextToIdMap, phraseTextToPhraseObjectMap = {}, {}
streamIdToStreamObjectMap = {}

def tweetsFromFile():
    for tweet in TweetFiles.iterateTweetsFromGzip('data/sample.gz'):
        message = TwitterCrowdsSpecificMethods.getMessageObjectForTweet(tweet, phraseTextToIdMap, phraseTextToPhraseObjectMap, **twitter_stream_settings)
        if message.streamId not in streamIdToStreamObjectMap: streamIdToStreamObjectMap[message.streamId] = Stream(message.streamId, message)
        else: streamIdToStreamObjectMap[message.streamId].updateForMessage(message, VectorUpdateMethods.exponentialDecay, **twitter_stream_settings )
        streamObject=streamIdToStreamObjectMap[message.streamId]
        print streamObject
        
    print len(phraseTextToIdMap)
    print len(phraseTextToPhraseObjectMap)
#    print pprint.pprint(phraseTextToIdMap)
    
if __name__ == '__main__':
    tweetsFromFile()
