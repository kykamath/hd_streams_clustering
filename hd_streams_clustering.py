'''
Created on Jun 25, 2011

@author: kykamath
'''
from classes import UtilityMethods, Stream, VectorUpdateMethods, StreamCluster
from library.classes import GeneralMethods, FixedIntervalMethod, timeit
from streaming_lsh.streaming_lsh_clustering import StreamingLSHClustering

#previousSet = set()

class DataStreamMethods:
    messageInOrderVariable = None
    @staticmethod
    def messageInOrder(messageTime):
        if DataStreamMethods.messageInOrderVariable==None or DataStreamMethods.messageInOrderVariable <= messageTime: DataStreamMethods.messageInOrderVariable = messageTime; return True
        else: return False
    @staticmethod
    def _resetClustersInSignatureTries(hdStreamClusteringObject, currentMessageTime):
        '''    This method clears the tries and puts current clusters into it. Do this everytime clusters or the dimensions change.    '''
        # Reset signature tries.
        for permutation in hdStreamClusteringObject.signaturePermutations: permutation.resetSignatureDataStructure()
        # Put current clusters into tries.
        for cluster in hdStreamClusteringObject.clusters.itervalues(): 
            cluster.setSignatureUsingVectorPermutations(hdStreamClusteringObject.unitVector, hdStreamClusteringObject.vectorPermutations, hdStreamClusteringObject.phraseTextAndDimensionMap)
            for permutation in hdStreamClusteringObject.signaturePermutations: permutation.addDocument(cluster)
    @staticmethod
    @timeit
    def updateDimensions(hdStreamClusteringObject, currentMessageTime): 
        # Update dimensions.
        UtilityMethods.updateDimensions(hdStreamClusteringObject.phraseTextAndDimensionMap, hdStreamClusteringObject.phraseTextToPhraseObjectMap, currentMessageTime, **hdStreamClusteringObject.stream_settings)
        DataStreamMethods._resetClustersInSignatureTries(hdStreamClusteringObject, currentMessageTime)
        
    @staticmethod
    @timeit
    def clusterFilteringMethod(hdStreamClusteringObject, currentMessageTime):
        for cluster in StreamCluster.getClustersByAttributeAndThreshold(hdStreamClusteringObject.clusters.values(), hdStreamClusteringObject.stream_settings['cluster_filter_attribute'], 
                                                                  hdStreamClusteringObject.stream_settings['cluster_filter_threshold'], StreamCluster.BELOW_THRESHOLD): del hdStreamClusteringObject.clusters[cluster.clusterId]
        for cluster in StreamCluster.getClustersByAttributeAndThreshold(hdStreamClusteringObject.clusters.values(), 'lastStreamAddedTime', 
                                                                  currentMessageTime-hdStreamClusteringObject.stream_settings['cluster_inactivity_time_in_seconds'], StreamCluster.BELOW_THRESHOLD): del hdStreamClusteringObject.clusters[cluster.clusterId]
        if hdStreamClusteringObject.combineClustersMethod!=None: hdStreamClusteringObject.clusters=hdStreamClusteringObject.combineClustersMethod(hdStreamClusteringObject.clusters, **hdStreamClusteringObject.stream_settings)
        DataStreamMethods._resetClustersInSignatureTries(hdStreamClusteringObject, currentMessageTime)
    @staticmethod
    def clusterAnalysisMethod(hdStreamClusteringObject, currentMessageTime): print 'shdnt come here'

class HDStreaminClustering(StreamingLSHClustering):
    def __init__(self, **stream_settings):
        super(HDStreaminClustering, self).__init__(**stream_settings)
        self.stream_settings = stream_settings
        self.phraseTextToPhraseObjectMap, self.streamIdToStreamObjectMap = {}, {}
        
        self.dimensionsUpdatingFrequency = stream_settings['dimension_update_frequency_in_seconds']
        self.clustersAnalysisFrequency = stream_settings['cluster_analysis_frequency_in_seconds']
        self.clustersFilteringFrequency = stream_settings['cluster_filtering_frequency_in_seconds']

        self.updateDimensionsMethod = FixedIntervalMethod(stream_settings.get('update_dimensions_method', DataStreamMethods.updateDimensions), self.dimensionsUpdatingFrequency)
        self.clusterAnalysisMethod = FixedIntervalMethod(stream_settings.get('cluster_analysis_method', DataStreamMethods.clusterAnalysisMethod), self.clustersAnalysisFrequency)
        self.clusterFilteringMethod = FixedIntervalMethod(stream_settings.get('cluster_filtering_method', DataStreamMethods.clusterFilteringMethod), self.clustersFilteringFrequency)
        
        self.combineClustersMethod=stream_settings.get('combine_clusters_method',None)
        self.convertDataToMessageMethod=stream_settings['convert_data_to_message_method']
        
        DataStreamMethods.messageInOrderVariable = None
    
    def cluster(self, dataIterator):
        i=0
        for data in dataIterator:
            message = self.convertDataToMessageMethod(data, **self.stream_settings)
            if DataStreamMethods.messageInOrder(message.timeStamp):
                UtilityMethods.updatePhraseTextToPhraseObject(message.vector, message.timeStamp, self.phraseTextToPhraseObjectMap, **self.stream_settings)
                if message.streamId not in self.streamIdToStreamObjectMap: self.streamIdToStreamObjectMap[message.streamId] = Stream(message.streamId, message)
#                else: self.streamIdToStreamObjectMap[message.streamId].updateForMessage(message, VectorUpdateMethods.exponentialDecay, **self.stream_settings )
                streamObject=self.streamIdToStreamObjectMap[message.streamId]
#                self.updateDimensionsMethod.call(message.timeStamp, hdStreamClusteringObject=self, currentMessageTime=message.timeStamp)
#                self.clusterFilteringMethod.call(message.timeStamp, hdStreamClusteringObject=self, currentMessageTime=message.timeStamp)
#                self.clusterAnalysisMethod.call(message.timeStamp, hdStreamClusteringObject=self, currentMessageTime=message.timeStamp)
                i+=1
#                print i, streamObject.lastMessageTime
                self.getClusterAndUpdateExistingClusters(streamObject)
    def getClusterAndUpdateExistingClusters(self, stream):
        predictedCluster = self.getClusterForDocument(stream)
        '''
        Do not remove this comment. Might need this if StreamCluster is used again in future.
        if predictedCluster!=None: self.clusters[predictedCluster].addStream(stream, **self.stream_settings)
        '''
        if predictedCluster!=None: self.clusters[predictedCluster].addDocument(stream, **self.stream_settings)
        else:
            newCluster = StreamCluster(stream)
            newCluster.setSignatureUsingVectorPermutations(self.unitVector, self.vectorPermutations, self.phraseTextAndDimensionMap)
            for permutation in self.signaturePermutations: permutation.addDocument(newCluster)
            self.clusters[newCluster.clusterId] = newCluster
#    def resetDatastructures(self, occuranceTime):
#        '''
#        1. Reset signature permutation trie.
#        2. Update cluster scores for all clusters.
#        3. Remove old clusters below a threshold.
#        4. Add every cluster to all the newly set signature permutation tries. 
#        '''
#        for permutation in self.signaturePermutations: permutation.resetSignatureDataStructure()
#        '''
#        Do not remove this comment. Might need this if StreamCluster is used again in future.
#        for cluster in self.clusters.itervalues(): cluster.updateScore(occuranceTime, 0, **self.stream_settings)
#        '''
#        for cluster in StreamCluster.getClustersByAttributeAndThreshold(self.clusters.values(), 
#                                                                  self.stream_settings['cluster_filter_attribute'], 
#                                                                  self.stream_settings['cluster_filter_threshold'], StreamCluster.BELOW_THRESHOLD): del self.clusters[cluster.clusterId]
#        if self.combineClustersMethod!=None: self.clusters=self.combineClustersMethod(self.clusters, **self.stream_settings)
#        for cluster in self.clusters.itervalues(): 
#            cluster.setSignatureUsingVectorPermutations(self.unitVector, self.vectorPermutations, self.phraseTextAndDimensionMap)
#            for permutation in self.signaturePermutations: permutation.addDocument(cluster)
    
