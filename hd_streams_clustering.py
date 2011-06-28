'''
Created on Jun 25, 2011

@author: kykamath
'''
from classes import UtilityMethods, Stream, VectorUpdateMethods, StreamCluster
from library.classes import GeneralMethods
from streaming_lsh.streaming_lsh_clustering import StreamingLSHClustering

class DataStreamMethods:
    messageInOrderVariable = None
    @staticmethod
    def messageInOrder(messageTime):
        if DataStreamMethods.messageInOrderVariable==None or DataStreamMethods.messageInOrderVariable <= messageTime: DataStreamMethods.messageInOrderVariable = messageTime; return True
        else: return False
    @staticmethod
    def updateDimensions(phraseTextAndDimensionMap, phraseTextToPhraseObjectMap, currentMessageTime, hdStreamClusteringObject, stream_settings): 
        print 'Entering:', currentMessageTime, len(phraseTextAndDimensionMap), len(phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
        UtilityMethods.updateForNewDimensions(phraseTextAndDimensionMap, phraseTextToPhraseObjectMap, currentMessageTime, **stream_settings)
        print 'Leaving: ', currentMessageTime, len(phraseTextAndDimensionMap), len(phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)

class HDStreaminClustering(StreamingLSHClustering):
    def __init__(self, **stream_settings):
        super(HDStreaminClustering, self).__init__(**stream_settings)
        self.stream_settings = stream_settings
        self.phraseTextToPhraseObjectMap, self.streamIdToStreamObjectMap = {}, {}
        self.dimensionUpdatingFrequency = stream_settings['dimension_update_frequency_in_seconds']
    def cluster(self, dataIterator, convertDataToMessage):
        i=0
        for data in dataIterator:
            message = convertDataToMessage(data, **self.stream_settings)
            if DataStreamMethods.messageInOrder(message.timeStamp):
                UtilityMethods.updatePhraseTextToPhraseObject(message.vector, message.timeStamp, self.phraseTextToPhraseObjectMap, **self.stream_settings)
                if message.streamId not in self.streamIdToStreamObjectMap: self.streamIdToStreamObjectMap[message.streamId] = Stream(message.streamId, message)
                else: self.streamIdToStreamObjectMap[message.streamId].updateForMessage(message, VectorUpdateMethods.exponentialDecay, **self.stream_settings )
                streamObject=self.streamIdToStreamObjectMap[message.streamId]
                GeneralMethods.callMethodEveryInterval(DataStreamMethods.updateDimensions, self.dimensionUpdatingFrequency, message.timeStamp, 
                                                       phraseTextAndDimensionMap=self.phraseTextAndDimensionMap, 
                                                       phraseTextToPhraseObjectMap=self.phraseTextToPhraseObjectMap,
                                                       currentMessageTime=message.timeStamp,
                                                       hdStreamClusteringObject=self,
                                                       stream_settings=self.stream_settings)
                print i, streamObject.lastMessageTime, len(self.clusters)
                i+=1
                self.getClusterAndUpdateExistingClusters(streamObject)
    def getClusterAndUpdateExistingClusters(self, stream):
        predictedCluster = self.getClusterForDocument(stream)
        if predictedCluster!=None:
            self.clusters[predictedCluster].addStream(stream)
        else:
            newCluster = StreamCluster(stream)
            newCluster.setSignatureUsingVectorPermutations(self.unitVector, self.vectorPermutations, self.phraseTextAndDimensionMap)
            for permutation in self.signaturePermutations: permutation.addStream(newCluster)
            self.clusters[newCluster.clusterId] = newCluster
