'''
Created on Jul 16, 2011

@author: kykamath
'''
import sys
from library.classes import GeneralMethods
sys.path.append('../')
from itertools import combinations
from collections import defaultdict
from twitter_streams_clustering import getExperts, TwitterCrowdsSpecificMethods
from settings import experts_twitter_stream_settings
from library.twitter import TweetFiles
from library.vector import Vector

clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'
clustering_quality_experts_sst_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_ssa_folder/'

class ItemsClusterer:
    def __init__(self):
        self.clusters = defaultdict(set)
        self.itemToClusterMap = {}
    def add(self, items):
        possibleClusters = set([self.itemToClusterMap.get(k) for k in items if k in self.itemToClusterMap])
        if not possibleClusters: return self.addToExistingCluster(list(items)[0], items)
        elif len(possibleClusters)==1: return self.addToExistingCluster(list(possibleClusters)[0], items)
        else: return self.mergeClusters(possibleClusters, items)
        raise Exception('Code shouldn\'t come here')
    def addToExistingCluster(self, clusterId, items):
        self.clusters[clusterId]=self.clusters[clusterId].union(items)
        for item in items: self.itemToClusterMap[item]=clusterId
    def mergeClusters(self, clusterIds, items):
        clusterIdsList=list(clusterIds)
        mainClusterId = clusterIdsList[0]
        self.addToExistingCluster(mainClusterId, items)
        for clusterId in clusterIdsList[1:]: 
            self.addToExistingCluster(mainClusterId, self.clusters[clusterId])
            del self.clusters[clusterId]
    def iterateClusters(self):
        for cluster in self.clusters.values(): yield cluster

class SimilarStreamAggregation:
    def __init__(self, vectors, epsilon): self.vectors, self.epsilon, self.clusterer = vectors, epsilon, ItemsClusterer()
    def estimate(self):
        similarVectorMap = self._getVectorsWithinEpsilon()
        for k, v in similarVectorMap.iteritems(): self.clusterer.add(set([k]).union(v))
    def iterateClusters(self): return self.clusterer.iterateClusters()
    def _getVectorsWithinEpsilon(self):
        vectorsWithinEpsilon = defaultdict(set)
        for id1, id2 in combinations(self.vectors,2):
            if  self.vectors[id1].cosineSimilarity(self.vectors[id2])>=self.epsilon: vectorsWithinEpsilon[id1].add(id2), vectorsWithinEpsilon[id2].add(id1)
        return vectorsWithinEpsilon
    
class TweetsFile:
    stats_file = clustering_quality_experts_sst_folder+'quality_stats'
    def __init__(self, length, **stream_settings):
        self.length=length
        self.stream_settings = stream_settings
        self.rawDataFileName = clustering_quality_experts_folder+'data/%s.gz'%str(length)
        self.expertsToClassMap = dict([(k, v['class']) for k,v in getExperts(byScreenName=True).iteritems()])
    def _iterateUserDocuments(self):
        dataForAggregation = defaultdict(Vector)
        textToIdMap = defaultdict(int)
        for tweet in TweetFiles.iterateTweetsFromGzip(self.rawDataFileName):
            textVector = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage(tweet, **self.stream_settings).vector
            textIdVector = Vector()
            for phrase in textVector: 
                if phrase not in textToIdMap: textToIdMap[phrase]=len(textToIdMap)
                textIdVector[textToIdMap[phrase]]=textVector[phrase]
            dataForAggregation[tweet['user']['screen_name'].lower()]+=textIdVector
        for k, v in dataForAggregation.iteritems(): yield k, v
    def getStatsForSST(self):
        sstObject = SimilarStreamAggregation(dict(self._iterateUserDocuments()), self.stream_settings['sst_threshold'])
        sstObject.estimate()
        distribution = GeneralMethods.getValueDistribution(sstObject.iterateClusters(), len)
        for k in distribution:
            print k, distribution[k]
        

if __name__ == '__main__':
    experts_twitter_stream_settings['sst_threshold']=0.75
    TweetsFile(1000, **experts_twitter_stream_settings).getStatsForSST()
        
    