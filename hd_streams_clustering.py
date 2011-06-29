'''
Created on Jun 25, 2011

@author: kykamath
'''
from classes import UtilityMethods, Stream, VectorUpdateMethods, StreamCluster
from library.classes import GeneralMethods
from streaming_lsh.streaming_lsh_clustering import StreamingLSHClustering
from operator import itemgetter
import time
from streaming_lsh.classes import Cluster

class DataStreamMethods:
    messageInOrderVariable = None
    @staticmethod
    def messageInOrder(messageTime):
        if DataStreamMethods.messageInOrderVariable==None or DataStreamMethods.messageInOrderVariable <= messageTime: DataStreamMethods.messageInOrderVariable = messageTime; return True
        else: return False
    @staticmethod
    def updateDimensions(phraseTextAndDimensionMap, phraseTextToPhraseObjectMap, currentMessageTime, hdStreamClusteringObject, stream_settings): 
        print '\n\n\nEntering:', currentMessageTime, len(phraseTextAndDimensionMap), len(phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
        UtilityMethods.updateDimensions(phraseTextAndDimensionMap, phraseTextToPhraseObjectMap, currentMessageTime, **stream_settings)
        hdStreamClusteringObject.resetDatastructures(currentMessageTime)
        hdStreamClusteringObject.printClusters()
        print 'Leaving: ', currentMessageTime, len(phraseTextAndDimensionMap), len(phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
        time.sleep(5)

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
#                UtilityMethods.updatePhraseTextToPhraseObject(message.vector, message.timeStamp, self.phraseTextToPhraseObjectMap, **self.stream_settings)
#                if message.streamId not in self.streamIdToStreamObjectMap: self.streamIdToStreamObjectMap[message.streamId] = Stream(message.streamId, message)
#                else: self.streamIdToStreamObjectMap[message.streamId].updateForMessage(message, VectorUpdateMethods.exponentialDecay, **self.stream_settings )
#                streamObject=self.streamIdToStreamObjectMap[message.streamId]
#                GeneralMethods.callMethodEveryInterval(DataStreamMethods.updateDimensions, self.dimensionUpdatingFrequency, message.timeStamp, 
#                                                       phraseTextAndDimensionMap=self.phraseTextAndDimensionMap, 
#                                                       phraseTextToPhraseObjectMap=self.phraseTextToPhraseObjectMap,
#                                                       currentMessageTime=message.timeStamp,
#                                                       hdStreamClusteringObject=self,
#                                                       stream_settings=self.stream_settings)
#                print i, streamObject.lastMessageTime, len(self.clusters)
                print i, message.timeStamp
                i+=1
#                self.getClusterAndUpdateExistingClusters(streamObject)
                
    def getClusterAndUpdateExistingClusters(self, stream):
        predictedCluster = self.getClusterForDocument(stream)
        if predictedCluster!=None: self.clusters[predictedCluster].addStream(stream, **self.stream_settings)
        else:
            newCluster = StreamCluster(stream)
            newCluster.setSignatureUsingVectorPermutations(self.unitVector, self.vectorPermutations, self.phraseTextAndDimensionMap)
            for permutation in self.signaturePermutations: permutation.addDocument(newCluster)
            self.clusters[newCluster.clusterId] = newCluster
            
    def resetDatastructures(self, occuranceTime):
        '''
        1. Reset signature permutation trie.
        2. Update cluster scores for all clusters.
        3. Remove old clusters below a threshold.
        4. Add every cluster to all the newly set signature permutation tries. 
        '''
        for permutation in self.signaturePermutations: permutation.resetSignatureTrie()
        for cluster in self.clusters.itervalues(): cluster.updateScore(occuranceTime, 0, **self.stream_settings)
        for cluster in Cluster.getClustersByAttributeAndThreshold(self.clusters.values(), 
                                                                  self.stream_settings['cluster_filter_attribute'], 
                                                                  self.stream_settings['cluster_filter_threshold'], Cluster.BELOW_THRESHOLD): del self.clusters[cluster.clusterId]
        for cluster in self.clusters.itervalues(): 
            cluster.setSignatureUsingVectorPermutations(self.unitVector, self.vectorPermutations, self.phraseTextAndDimensionMap)
            for permutation in self.signaturePermutations: permutation.addDocument(cluster)
    
    def printClusters(self):
        for cluster, _ in sorted(Cluster.iterateByAttribute(self.clusters.values(), 'length'), key=itemgetter(1), reverse=True)[:10]:
            print cluster.clusterId, cluster.length, cluster.score, [stream.docId for stream in cluster.iterateDocumentsInCluster()][:5], cluster.getTopDimensions(numberOfFeatures=5)
