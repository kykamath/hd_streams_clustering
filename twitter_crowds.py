'''
Created on Jun 22, 2011

@author: kykamath
'''
from settings import twitter_stream_settings
from library.twitter import TweetFiles, getDateTimeObjectFromTweetTimestamp
from library.classes import GeneralMethods
from classes import Message, UtilityMethods, Stream, VectorUpdateMethods
from datetime import timedelta

phraseTextToIdMap, phraseTextToPhraseObjectMap, streamIdToStreamObjectMap = {}, {}, {}
dimensionUpdatingFrequency = timedelta(twitter_stream_settings['time_unit_in_seconds'])

class TwitterCrowdsSpecificMethods:
    @staticmethod
    def getMessageObjectForTweet(tweet, phraseTextToIdMap, phraseTextToPhraseObjectMap, **twitter_stream_settings):
        tweetTime = getDateTimeObjectFromTweetTimestamp(tweet['created_at'])
        message = Message(tweet['user']['screen_name'], tweet['id'], tweet['text'], tweetTime)
        message.vector = UtilityMethods.getVectorForText(tweet['text'], tweetTime, phraseTextToIdMap, phraseTextToPhraseObjectMap, **twitter_stream_settings)
        return message
    @staticmethod
    def printParam(param): print param

def tweetsFromFile():
#    for tweet in TweetFiles.iterateTweetsFromGzip('data/sample.gz'):
    for tweet in TweetFiles.iterateTweetsFromGzip('/mnt/chevron/kykamath/data/twitter/filter/2011_2_6.gz'):
        message = TwitterCrowdsSpecificMethods.getMessageObjectForTweet(tweet, phraseTextToIdMap, phraseTextToPhraseObjectMap, **twitter_stream_settings)
        if message.streamId not in streamIdToStreamObjectMap: streamIdToStreamObjectMap[message.streamId] = Stream(message.streamId, message)
        else: streamIdToStreamObjectMap[message.streamId].updateForMessage(message, VectorUpdateMethods.exponentialDecay, **twitter_stream_settings )
        streamObject=streamIdToStreamObjectMap[message.streamId]
#        print streamObject
        GeneralMethods.callMethodEveryInterval(TwitterCrowdsSpecificMethods.printParam, timedelta(minutes=15), message.timeStamp, param=message.timeStamp)
        
    print len(phraseTextToIdMap)
    print len(phraseTextToPhraseObjectMap)
#    print pprint.pprint(phraseTextToIdMap)
    
if __name__ == '__main__':
    tweetsFromFile()
