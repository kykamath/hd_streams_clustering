'''
Created on Jun 22, 2011

@author: kykamath
'''
from settings import twitter_stream_settings
from library.twitter import TweetFiles, getDateTimeObjectFromTweetTimestamp
from library.classes import GeneralMethods
from classes import Message, UtilityMethods, Stream, VectorUpdateMethods
from HDStreamClustering import HDStreaminClustering

class TwitterStreamVariables:
    phraseTextToIdMap, phraseTextToPhraseObjectMap, streamIdToStreamObjectMap = {}, {}, {}
    dimensionUpdatingFrequency = twitter_stream_settings['dimension_update_frequency_in_seconds']

class TwitterIterator:
    '''
    Returns a tweet on every iteration. This iterator will be used to cluster streams.
    To use this with adifferent data soruce, ex. Twitter Streaming API, write a iterator for the
    data source accordingly.
    '''
    @staticmethod
    def iterateFromFile(file):
        for tweet in TweetFiles.iterateTweetsFromGzip(file): yield tweet
        

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
        print 'Entering:', currentMessageTime, len(phraseTextToIdMap), len(phraseTextToPhraseObjectMap)
        UtilityMethods.updateForNewDimensions(phraseTextToIdMap, phraseTextToPhraseObjectMap, currentMessageTime, **twitter_stream_settings)
        print 'Leaving: ', currentMessageTime, len(phraseTextToIdMap), len(phraseTextToPhraseObjectMap)

        
def clusterTwitterStreams():
    hdStreamClusteringObject = HDStreaminClustering()
#    for tweet in TwitterIterator.iterateFromFile('data/sample.gz'):
    for tweet in TwitterIterator.iterateFromFile('/mnt/chevron/kykamath/data/twitter/filter/2011_2_6.gz'):
        message = TwitterCrowdsSpecificMethods.getMessageObjectForTweet(tweet, TwitterStreamVariables.phraseTextToIdMap, TwitterStreamVariables.phraseTextToPhraseObjectMap, **twitter_stream_settings)
        if TwitterCrowdsSpecificMethods.messageInOrder(message.timeStamp):
            if message.streamId not in TwitterStreamVariables.streamIdToStreamObjectMap: TwitterStreamVariables.streamIdToStreamObjectMap[message.streamId] = Stream(message.streamId, message)
            else: TwitterStreamVariables.streamIdToStreamObjectMap[message.streamId].updateForMessage(message, VectorUpdateMethods.exponentialDecay, **twitter_stream_settings )
            streamObject=TwitterStreamVariables.streamIdToStreamObjectMap[message.streamId]
            GeneralMethods.callMethodEveryInterval(TwitterCrowdsSpecificMethods.updateDimensions, TwitterStreamVariables.dimensionUpdatingFrequency, message.timeStamp, 
                                                   phraseTextToIdMap=TwitterStreamVariables.phraseTextToIdMap, 
                                                   phraseTextToPhraseObjectMap=TwitterStreamVariables.phraseTextToPhraseObjectMap,
                                                   currentMessageTime=message.timeStamp)
            print streamObject.lastMessageTime 
if __name__ == '__main__':
    clusterTwitterStreams()
