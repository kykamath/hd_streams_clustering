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
    def updateDimensions(hdStreamClusteringObject, currentMessageTime): 
#        print '\n\n\nEntering:', currentMessageTime, len(hdStreamClusteringObject.phraseTextAndDimensionMap), len(hdStreamClusteringObject.phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
        UtilityMethods.updateDimensions(hdStreamClusteringObject.phraseTextAndDimensionMap, hdStreamClusteringObject.phraseTextToPhraseObjectMap, currentMessageTime, **hdStreamClusteringObject.stream_settings)
        hdStreamClusteringObject.resetDatastructures(currentMessageTime)
        if hdStreamClusteringObject.analyzeIterationDataMethod!=None: hdStreamClusteringObject.analyzeIterationDataMethod(hdStreamClusteringObject, currentMessageTime)
#        print 'Leaving: ', currentMessageTime, len(hdStreamClusteringObject.phraseTextAndDimensionMap), len(hdStreamClusteringObject.phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)

class HDStreaminClustering(StreamingLSHClustering):
    def __init__(self, **stream_settings):
        super(HDStreaminClustering, self).__init__(**stream_settings)
        self.stream_settings = stream_settings
        self.phraseTextToPhraseObjectMap, self.streamIdToStreamObjectMap = {}, {}
        self.dimensionUpdatingFrequency = stream_settings['dimension_update_frequency_in_seconds']
        self.convertDataToMessageMethod=stream_settings['convert_data_to_message_method']
        self.combineClustersMethod=stream_settings.get('combine_clusters_method',None)
        self.analyzeIterationDataMethod=stream_settings.get('analyze_iteration_data_method',None)
    def cluster(self, dataIterator):
        i=0
        for data in dataIterator:
            message = self.convertDataToMessageMethod(data, **self.stream_settings)
            if DataStreamMethods.messageInOrder(message.timeStamp):
                UtilityMethods.updatePhraseTextToPhraseObject(message.vector, message.timeStamp, self.phraseTextToPhraseObjectMap, **self.stream_settings)
                if message.streamId not in self.streamIdToStreamObjectMap: self.streamIdToStreamObjectMap[message.streamId] = Stream(message.streamId, message)
                else: self.streamIdToStreamObjectMap[message.streamId].updateForMessage(message, VectorUpdateMethods.exponentialDecay, **self.stream_settings )
                streamObject=self.streamIdToStreamObjectMap[message.streamId]
                GeneralMethods.callMethodEveryInterval(DataStreamMethods.updateDimensions, self.dimensionUpdatingFrequency, message.timeStamp, 
                                                       hdStreamClusteringObject=self,
                                                       currentMessageTime=message.timeStamp)
                i+=1
                print i, streamObject.lastMessageTime
                self.getClusterAndUpdateExistingClusters(streamObject)
    def getClusterAndUpdateExistingClusters(self, stream):
        predictedCluster = self.getClusterForDocument(stream)
        '''
        Do not remove this comment. Might need this if StreamCluster is used again in future.
        if predictedCluster!=None: self.clusters[predictedCluster].addStream(stream, **self.stream_settings)
        '''
        if predictedCluster!=None: self.clusters[predictedCluster].addDocument(stream)
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
        '''
        Do not remove this comment. Might need this if StreamCluster is used again in future.
        for cluster in self.clusters.itervalues(): cluster.updateScore(occuranceTime, 0, **self.stream_settings)
        '''
        for cluster in StreamCluster.getClustersByAttributeAndThreshold(self.clusters.values(), 
                                                                  self.stream_settings['cluster_filter_attribute'], 
                                                                  self.stream_settings['cluster_filter_threshold'], StreamCluster.BELOW_THRESHOLD): del self.clusters[cluster.clusterId]
        if self.combineClustersMethod!=None: self.clusters=self.combineClustersMethod(self.clusters, **self.stream_settings)
        for cluster in self.clusters.itervalues(): 
            cluster.setSignatureUsingVectorPermutations(self.unitVector, self.vectorPermutations, self.phraseTextAndDimensionMap)
            for permutation in self.signaturePermutations: permutation.addDocument(cluster)
