'''
Created on Jun 22, 2011

@author: kykamath
'''
from settings import twitter_stream_settings
from library.twitter import TweetFiles, getDateTimeObjectFromTweetTimestamp
from library.classes import GeneralMethods
from classes import Message, UtilityMethods, Stream, VectorUpdateMethods
from HDStreamClustering import HDStreaminClustering
from collections import defaultdict
from datetime import datetime, timedelta
from collections import defaultdict
import pprint
from streaming_lsh.library.file_io import FileIO

def getExperts():
    usersList, usersData = {}, defaultdict(list)
    for l in open(twitter_stream_settings.usersToCrawl): data = l.strip().split(); usersData[data[0]].append(data[1:])
    for k, v in usersData.iteritems(): 
        for user in v: usersList[user[1]] = {'screen_name': user[0], 'class':k}
    return usersList

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
    @staticmethod
    def iterateTweetsFromExperts(expertsDataStartTime=datetime(2011,3,19), expertsDataEndTime=datetime(2011,4,12)):
        experts = getExperts()
        currentTime = expertsDataStartTime
        while currentTime <= expertsDataEndTime:
            for tweet in TwitterIterator.iterateFromFile(twitter_stream_settings.twitterUsersTweetsFolder+'%s.gz'%FileIO.getFileByDay(currentTime)):
                if tweet['user']['id_str'] in experts: yield tweet
            currentTime+=timedelta(days=1)

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
    def updateDimensions(phraseTextToIdMap, phraseTextToPhraseObjectMap, currentMessageTime, hdStreamClusteringObject): 
        print 'Entering:', currentMessageTime, len(phraseTextToIdMap), len(phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
        UtilityMethods.updateForNewDimensions(phraseTextToIdMap, phraseTextToPhraseObjectMap, currentMessageTime, **twitter_stream_settings)
        print 'Leaving: ', currentMessageTime, len(phraseTextToIdMap), len(phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
        
def clusterTwitterStreams():
    hdStreamClusteringObject = HDStreaminClustering(**twitter_stream_settings)
#    for tweet in TwitterIterator.iterateFromFile('/mnt/chevron/kykamath/temp_data/sample.gz'):
    i = 0
    for tweet in TwitterIterator.iterateFromFile('/mnt/chevron/kykamath/data/twitter/filter/2011_2_6.gz'):
        message = TwitterCrowdsSpecificMethods.getMessageObjectForTweet(tweet, TwitterStreamVariables.phraseTextToIdMap, TwitterStreamVariables.phraseTextToPhraseObjectMap, **twitter_stream_settings)
        if TwitterCrowdsSpecificMethods.messageInOrder(message.timeStamp):
            if message.streamId not in TwitterStreamVariables.streamIdToStreamObjectMap: TwitterStreamVariables.streamIdToStreamObjectMap[message.streamId] = Stream(message.streamId, message)
            else: TwitterStreamVariables.streamIdToStreamObjectMap[message.streamId].updateForMessage(message, VectorUpdateMethods.exponentialDecay, **twitter_stream_settings )
            streamObject=TwitterStreamVariables.streamIdToStreamObjectMap[message.streamId]
            GeneralMethods.callMethodEveryInterval(TwitterCrowdsSpecificMethods.updateDimensions, TwitterStreamVariables.dimensionUpdatingFrequency, message.timeStamp, 
                                                   phraseTextToIdMap=TwitterStreamVariables.phraseTextToIdMap, 
                                                   phraseTextToPhraseObjectMap=TwitterStreamVariables.phraseTextToPhraseObjectMap,
                                                   currentMessageTime=message.timeStamp,
                                                   hdStreamClusteringObject=hdStreamClusteringObject)
            print i, streamObject.lastMessageTime, len(hdStreamClusteringObject.clusters)
            i+=1
            hdStreamClusteringObject.getClusterAndUpdateExistingClusters(streamObject)
            
if __name__ == '__main__':
    clusterTwitterStreams()
