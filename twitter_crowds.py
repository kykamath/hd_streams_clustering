'''
Created on Jun 22, 2011

@author: kykamath
'''
from settings import twitter_stream_settings
from library.twitter import TweetFiles, getDateTimeObjectFromTweetTimestamp
from library.classes import GeneralMethods
from classes import Message, UtilityMethods, Stream, VectorUpdateMethods
from datetime import timedelta

class TwitterStreamVariables:
    phraseTextToIdMap, phraseTextToPhraseObjectMap, streamIdToStreamObjectMap = {}, {}, {}
    dimensionUpdatingFrequency = timedelta(twitter_stream_settings['time_unit_in_seconds'])
#    dimensionUpdatingFrequency = timedelta(seconds=60)

class TwitterCrowdsSpecificMethods:
    @staticmethod
    def getMessageObjectForTweet(tweet, phraseTextToIdMap, phraseTextToPhraseObjectMap, **twitter_stream_settings):
        tweetTime = getDateTimeObjectFromTweetTimestamp(tweet['created_at'])
        message = Message(tweet['user']['screen_name'], tweet['id'], tweet['text'], tweetTime)
        message.vector = UtilityMethods.getVectorForText(tweet['text'], tweetTime, phraseTextToIdMap, phraseTextToPhraseObjectMap, **twitter_stream_settings)
        return message
    @staticmethod
    def printParam(phraseTextToIdMap, phraseTextToPhraseObjectMap, currentTime): 
        print 'comes here', currentTime
#        UtilityMethods.updateForNewDimensions(phraseTextToIdMap, phraseTextToPhraseObjectMap, currentTime, **twitter_stream_settings)
#        print len(phraseTextToIdMap), len(phraseTextToPhraseObjectMap)

def tweetsFromFile():
#    for tweet in TweetFiles.iterateTweetsFromGzip('data/sample.gz'):
    for tweet in TweetFiles.iterateTweetsFromGzip('/mnt/chevron/kykamath/data/twitter/filter/2011_2_6.gz'):
        message = TwitterCrowdsSpecificMethods.getMessageObjectForTweet(tweet, TwitterStreamVariables.phraseTextToIdMap, TwitterStreamVariables.phraseTextToPhraseObjectMap, **twitter_stream_settings)
        if message.streamId not in TwitterStreamVariables.streamIdToStreamObjectMap: TwitterStreamVariables.streamIdToStreamObjectMap[message.streamId] = Stream(message.streamId, message)
        else: TwitterStreamVariables.streamIdToStreamObjectMap[message.streamId].updateForMessage(message, VectorUpdateMethods.exponentialDecay, **twitter_stream_settings )
        streamObject=TwitterStreamVariables.streamIdToStreamObjectMap[message.streamId]
#        print streamObject.lastMessageTime
        GeneralMethods.callMethodEveryInterval(TwitterCrowdsSpecificMethods.printParam, TwitterStreamVariables.dimensionUpdatingFrequency, message.timeStamp, 
                                               phraseTextToIdMap=TwitterStreamVariables.phraseTextToIdMap, 
                                               phraseTextToPhraseObjectMap=TwitterStreamVariables.phraseTextToPhraseObjectMap,
                                               currentTime=message.timeStamp)
        
if __name__ == '__main__':
    tweetsFromFile()
