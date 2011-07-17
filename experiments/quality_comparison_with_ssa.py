'''
Created on Jul 16, 2011

@author: kykamath
'''
import sys, time
sys.path.append('../')
from library.clustering import EvaluationMetrics
from experiments.ssa.ssa import SimilarStreamAggregation,\
    StreamSimilarityAggregationMR
from collections import defaultdict
from twitter_streams_clustering import getExperts, TwitterCrowdsSpecificMethods
from settings import experts_twitter_stream_settings
from library.twitter import TweetFiles
from library.vector import Vector

clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'
clustering_quality_experts_ssa_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_ssa_folder/'
clustering_quality_experts_ssa_mr_folder = clustering_quality_experts_ssa_folder+'mr_data/'
hdfsPath='hdfs:///user/kykamath/lsh_experts_data/clustering_quality_ssa_folder'

class TweetsFile:
    stats_file = clustering_quality_experts_ssa_folder+'quality_stats'
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
    def _getExpertClasses(self, cluster): return [self.expertsToClassMap[user.lower()] for user in cluster if user.lower() in self.expertsToClassMap]
    def getEvaluationMetrics(self, documentClusters, timeDifference):
        iterationData =  {'no_of_documents':self.length, 'no_of_clusters': len(documentClusters), 'iteration_time': timeDifference, 'clusters': documentClusters}
        clustersForEvaluation = [self._getExpertClasses(cluster) for cluster in documentClusters]
        iterationData['nmi'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.nmi)
        iterationData['purity'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.purity)
        iterationData['f1'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.f1)
        return iterationData
    def getStatsForSSA(self):
        ts = time.time()
        sstObject = SimilarStreamAggregation(dict(self._iterateUserDocuments()), self.stream_settings['ssa_threshold'])
        sstObject.estimate()
        documentClusters = list(sstObject.iterateClusters())
        te = time.time()
        return self.getEvaluationMetrics(documentClusters, te-ts)
#    def getStatsForSSAMR(self):
#        ts = time.time()
#        documentClusters = StreamSimilarityAggregationMR.estimate(test_file, '-r hadoop'.split(), jobconf={'mapred.reduce.tasks':2})
#        te = time.time()
#        return self.getEvaluationMetrics(documentClusters, te-ts)
    @staticmethod
    def generateDocsForSSAMR():
        length=1000
        tf = TweetsFile(2000, **experts_twitter_stream_settings)
        for d in tf._iterateUserDocuments():
            print d
if __name__ == '__main__':
    experts_twitter_stream_settings['ssa_threshold']=0.75
    print TweetsFile(2000, **experts_twitter_stream_settings).getStatsForSSA()
        
    