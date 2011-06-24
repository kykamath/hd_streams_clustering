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
    dimensionUpdatingFrequency = timedelta(seconds=twitter_stream_settings['time_unit_in_seconds'])
#    dimensionUpdatingFrequency = timedelta(seconds=60)

class TwitterCrowdsSpecificMethods:
    messageInOrderVariable = None
    @staticmethod
    def messageInOrder(messageTime):
        if TwitterCrowdsSpecificMethods.messageInOrderVariable==None or TwitterCrowdsSpecificMethods.messageInOrderVariable <= messageTime: TwitterCrowdsSpecificMethods.messageInOrderVariable = messageTime; return True
        else: return False
    @staticmethod
    def getMessageObjectForTweet(tweet, phraseTextToIdMap, phraseTextToPhraseObjectMap, **twitter_stream_settings):
        tweetTime = getDateTimeObjectFromTweetTimestamp(tweet['created_at'])
        message = Message(tweet['user']['screen_name'], tweet['id'], tweet['text'], tweetTime)
        message.vector = UtilityMethods.getVectorForText(tweet['text'], tweetTime, phraseTextToIdMap, phraseTextToPhraseObjectMap, **twitter_stream_settings)
        return message
    @staticmethod
    def updateDimensions(phraseTextToIdMap, phraseTextToPhraseObjectMap, currentMessageTime): 
        print 'comes here', currentMessageTime, len(phraseTextToIdMap), len(phraseTextToPhraseObjectMap)
        UtilityMethods.updateForNewDimensions(phraseTextToIdMap, phraseTextToPhraseObjectMap, currentMessageTime, **twitter_stream_settings)

def tweetsFromFile():
#    for tweet in TweetFiles.iterateTweetsFromGzip('data/sample.gz'):
    for tweet in TweetFiles.iterateTweetsFromGzip('/mnt/chevron/kykamath/data/twitter/filter/2011_2_6.gz'):
        message = TwitterCrowdsSpecificMethods.getMessageObjectForTweet(tweet, TwitterStreamVariables.phraseTextToIdMap, TwitterStreamVariables.phraseTextToPhraseObjectMap, **twitter_stream_settings)
        if TwitterCrowdsSpecificMethods.messageInOrder(message.timeStamp):
            if message.streamId not in TwitterStreamVariables.streamIdToStreamObjectMap: TwitterStreamVariables.streamIdToStreamObjectMap[message.streamId] = Stream(message.streamId, message)
            else: TwitterStreamVariables.streamIdToStreamObjectMap[message.streamId].updateForMessage(message, VectorUpdateMethods.exponentialDecay, **twitter_stream_settings )
#            streamObject=TwitterStreamVariables.streamIdToStreamObjectMap[message.streamId]
#            print streamObject.lastMessageTime
            GeneralMethods.callMethodEveryInterval(TwitterCrowdsSpecificMethods.updateDimensions, TwitterStreamVariables.dimensionUpdatingFrequency, message.timeStamp, 
                                                   phraseTextToIdMap=TwitterStreamVariables.phraseTextToIdMap, 
                                                   phraseTextToPhraseObjectMap=TwitterStreamVariables.phraseTextToPhraseObjectMap,
                                                   currentMessageTime=message.timeStamp)
            
if __name__ == '__main__':
    tweetsFromFile()
