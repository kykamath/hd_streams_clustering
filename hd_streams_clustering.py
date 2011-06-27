'''
Created on Jun 25, 2011

@author: kykamath
'''
from streaming_lsh.StreamingLSHClustering import StreamingLSHClustering
from classes import UtilityMethods, Stream, VectorUpdateMethods
from library.classes import GeneralMethods

class DataStreamMethods:
    messageInOrderVariable = None
    @staticmethod
    def messageInOrder(messageTime):
        if DataStreamMethods.messageInOrderVariable==None or DataStreamMethods.messageInOrderVariable <= messageTime: DataStreamMethods.messageInOrderVariable = messageTime; return True
        else: return False
    @staticmethod
    def updateDimensions(phraseTextToIdMap, phraseTextToPhraseObjectMap, currentMessageTime, hdStreamClusteringObject, **stream_settings): 
        print 'Entering:', currentMessageTime, len(phraseTextToIdMap), len(phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
        UtilityMethods.updateForNewDimensions(phraseTextToIdMap, phraseTextToPhraseObjectMap, currentMessageTime, **stream_settings)
        print 'Leaving: ', currentMessageTime, len(phraseTextToIdMap), len(phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)

class HDStreaminClustering(StreamingLSHClustering):
    def __init__(self, **stream_settings):
        super(HDStreaminClustering, self).__init__(**stream_settings)
        self.stream_settings = stream_settings
        self.phraseTextToIdMap, self.phraseTextToPhraseObjectMap, self.streamIdToStreamObjectMap = {}, {}, {}
        self.dimensionUpdatingFrequency = stream_settings['dimension_update_frequency_in_seconds']
    def cluster(self, dataIterator, dataToMessageConverter):
        i=0
        for data in dataIterator:
            message = dataToMessageConverter(data, self.phraseTextToIdMap, self.phraseTextToPhraseObjectMap, **self.stream_settings)
            if DataStreamMethods.messageInOrder(message.timeStamp):
                if message.streamId not in self.streamIdToStreamObjectMap: self.streamIdToStreamObjectMap[message.streamId] = Stream(message.streamId, message)
                else: self.streamIdToStreamObjectMap[message.streamId].updateForMessage(message, VectorUpdateMethods.exponentialDecay, **self.stream_settings )
                streamObject=self.streamIdToStreamObjectMap[message.streamId]
                GeneralMethods.callMethodEveryInterval(DataStreamMethods.updateDimensions, self.dimensionUpdatingFrequency, message.timeStamp, 
                                                       phraseTextToIdMap=self.phraseTextToIdMap, 
                                                       phraseTextToPhraseObjectMap=self.phraseTextToPhraseObjectMap,
                                                       currentMessageTime=message.timeStamp,
                                                       hdStreamClusteringObject=self)
                print i, streamObject.lastMessageTime, len(self.clusters)
                i+=1
                self.getClusterAndUpdateExistingClusters(streamObject)
    
        

        
#def clusterTwitterStreams():
#    hdStreamClusteringObject = HDStreaminClustering(**twitter_stream_settings)
##    for tweet in TwitterIterator.iterateFromFile('/mnt/chevron/kykamath/temp_data/sample.gz'):
#    i = 0
#    for tweet in TwitterIterator.iterateFromFile('/mnt/chevron/kykamath/data/twitter/filter/2011_2_6.gz'):
#        message = TwitterCrowdsSpecificMethods.getMessageObjectForTweet(tweet, TwitterStreamVariables.phraseTextToIdMap, TwitterStreamVariables.phraseTextToPhraseObjectMap, **twitter_stream_settings)
#        if TwitterCrowdsSpecificMethods.messageInOrder(message.timeStamp):
#            if message.streamId not in TwitterStreamVariables.streamIdToStreamObjectMap: TwitterStreamVariables.streamIdToStreamObjectMap[message.streamId] = Stream(message.streamId, message)
#            else: TwitterStreamVariables.streamIdToStreamObjectMap[message.streamId].updateForMessage(message, VectorUpdateMethods.exponentialDecay, **twitter_stream_settings )
#            streamObject=TwitterStreamVariables.streamIdToStreamObjectMap[message.streamId]
#            GeneralMethods.callMethodEveryInterval(TwitterCrowdsSpecificMethods.updateDimensions, TwitterStreamVariables.dimensionUpdatingFrequency, message.timeStamp, 
#                                                   phraseTextToIdMap=TwitterStreamVariables.phraseTextToIdMap, 
#                                                   phraseTextToPhraseObjectMap=TwitterStreamVariables.phraseTextToPhraseObjectMap,
#                                                   currentMessageTime=message.timeStamp,
#                                                   hdStreamClusteringObject=hdStreamClusteringObject)
#            print i, streamObject.lastMessageTime, len(hdStreamClusteringObject.clusters)
#            i+=1
#            hdStreamClusteringObject.getClusterAndUpdateExistingClusters(streamObject)