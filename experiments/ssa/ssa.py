'''
Created on Jul 17, 2011

@author: kykamath
'''
from itertools import combinations
from collections import defaultdict
from library.mrjobwrapper import CJSONProtocol
from ssa_agg_mr import SSAAggrigationMR
from ssa_sim_mr import SSASimilarityMR

stream_in_multiple_clusters = 'stream_in_multiple_clusters'
iteration_file = '/tmp/ssa_iteration'


def createFileForNextIteration(data):
    with open(iteration_file, 'w') as fp: [fp.write(CJSONProtocol.write(k, v)+'\n') for k, v in data.iteritems()]

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
    def iterateClusters(self): 
        for c in self.clusterer.iterateClusters(): yield list(c)
    def _getVectorsWithinEpsilon(self):
        vectorsWithinEpsilon = defaultdict(set)
        for id1, id2 in combinations(self.vectors,2):
            if  self.vectors[id1].cosineSimilarity(self.vectors[id2])>=self.epsilon: vectorsWithinEpsilon[id1].add(id2), vectorsWithinEpsilon[id2].add(id1)
        return vectorsWithinEpsilon    

class StreamSimilarityAggregationMR:
    @staticmethod
    def estimate(fileName, maxItertations=None, args='-r local'.split(), **kwargs):
        similarityJob = SSASimilarityMR(args=args)
        dataFromIteration = dict(list(similarityJob.runJob(inputFileList=[fileName], **kwargs)))
        numberOfIterations = 0
        while stream_in_multiple_clusters in dataFromIteration:
            print 'SSA-MR Iteration %s'%numberOfIterations
            numberOfIterations+=1
            del dataFromIteration[stream_in_multiple_clusters]
            createFileForNextIteration(dataFromIteration)
            aggrigationJob = SSAAggrigationMR(args=args)
            dataFromIteration = dict(list(aggrigationJob.runJob(inputFileList=[iteration_file], **kwargs)))
            if maxItertations!=None and numberOfIterations>=maxItertations: print 'Reached max iterations. Breaking from the loop'; break;
        for id in dataFromIteration: yield [id]+dataFromIteration[id]['s']              
