'''
Created on Jul 16, 2011

@author: kykamath
'''
import sys, time, os
from library.file_io import FileIO
from library.classes import Settings
sys.path.append('../')
from library.clustering import EvaluationMetrics
from experiments.ssa.ssa import SimilarStreamAggregation,\
    StreamSimilarityAggregationMR
from collections import defaultdict
from twitter_streams_clustering import getExperts, TwitterCrowdsSpecificMethods
from settings import experts_twitter_stream_settings
from library.twitter import TweetFiles
from library.vector import Vector
from library.mrjobwrapper import CJSONProtocol
from itertools import combinations
from streaming_lsh.classes import Document
from streaming_lsh.streaming_lsh_clustering import StreamingLSHClustering
from quality_comparison_with_kmeans import TweetsFile as KMeansTweetsFile

clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'
clustering_quality_experts_ssa_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_ssa_folder/'
clustering_quality_experts_ssa_mr_folder = clustering_quality_experts_ssa_folder+'mr_data/'
hdfsPath='hdfs:///user/kykamath/lsh_experts_data/clustering_quality_ssa_folder/'
hdfsUnzippedPath='hdfs:///user/kykamath/lsh_experts_data/clustering_quality_ssa_unzipped_folder/'

class TweetsFile:
    stats_file = clustering_quality_experts_ssa_folder+'quality_stats'
    def __init__(self, length, **stream_settings):
        self.length=length
        self.stream_settings = stream_settings
        self.rawDataFileName = clustering_quality_experts_folder+'data/%s.gz'%str(length)
        self.expertsToClassMap = dict([(k, v['class']) for k,v in getExperts(byScreenName=True).iteritems()])
        self.hdfsFile = hdfsPath+'%s.gz'%length
    def _iterateUserDocuments(self):
        dataForAggregation = defaultdict(Vector)
        textToIdMap = defaultdict(int)
        for tweet in TweetFiles.iterateTweetsFromGzip(self.rawDataFileName):
            textVector = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage(tweet, **self.stream_settings).vector
            textIdVector = Vector()
            for phrase in textVector: 
                if phrase not in textToIdMap: textToIdMap[phrase]=str(len(textToIdMap))
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
        print 'SSA'
        ts = time.time()
        sstObject = SimilarStreamAggregation(dict(self._iterateUserDocuments()), self.stream_settings['ssa_threshold'])
        sstObject.estimate()
        documentClusters = list(sstObject.iterateClusters())
        te = time.time()
        return self.getEvaluationMetrics(documentClusters, te-ts)
    def getStatsForSSAMR(self):
        print 'SSA-MR'
        ts = time.time()
        documentClusters = list(StreamSimilarityAggregationMR.estimate(
                                                                       self.hdfsFile, args='-r hadoop'.split(), 
                                                                       jobconf={'mapred.map.tasks':25, 'mapred.task.timeout': 7200000, 'mapred.reduce.tasks':25}))
        te = time.time()
        return self.getEvaluationMetrics(documentClusters, te-ts)
    @staticmethod
    def generateDocsForSSAMR():
        for length in [i*j for i in 10**3, 10**4, 10**5 for j in range(1, 10)]: 
            tf = TweetsFile(length, **experts_twitter_stream_settings)
            iteration_file = clustering_quality_experts_ssa_mr_folder+str(length)
            print 'Generating data for ', iteration_file
            with open(iteration_file, 'w') as fp: [fp.write(CJSONProtocol.write('x', [doc1, doc2])+'\n') for doc1, doc2 in combinations(tf._iterateUserDocuments(),2)]
            os.system('gzip %s'%iteration_file)
            os.system('hadoop fs -put %s.gz %s'%(iteration_file, hdfsPath))
    @staticmethod
    def copyUnzippedSSADataToHadoop():
        for length in [i*j for i in 10**3, 10**4, 10**5 for j in range(1, 10)]: 
            iteration_file = clustering_quality_experts_ssa_mr_folder+str(length)
            print 'Copying file for %s'%length
            os.system('gunzip %s.gz'%iteration_file)
            os.system('hadoop fs -put %s %s'%(iteration_file, hdfsUnzippedPath))

class QualityComparisonWithSSA:
    @staticmethod
    def generateStatsForQualityComparisonWithSSA():
        for length in [5000, 6000, 7000, 8000, 9000]+[i*j for i in 10**4, 10**5 for j in range(1, 10)]:
            print 'Generating stats for: ',length
            tf = TweetsFile(length, **experts_twitter_stream_settings)
            stats = {'ssa': tf.getStatsForSSA(), 'ssa_mr': tf.getStatsForSSAMR(), 'streaming_lsh': KMeansTweetsFile(length, **experts_twitter_stream_settings).generateStatsForStreamingLSHClustering(), 'settings': Settings.getSerialzedObject(tf.stream_settings)}
            FileIO.writeToFileAsJson(stats, TweetsFile.stats_file)
if __name__ == '__main__':
    experts_twitter_stream_settings['ssa_threshold']=0.75
#    TweetsFile.generateDocsForSSAMR()
    TweetsFile.copyUnzippedSSADataToHadoop()
#    QualityComparisonWithSSA.generateStatsForQualityComparisonWithSSA()
#    tf = TweetsFile(5000, **experts_twitter_stream_settings)
#    tf.getStatsForSSAMR()
    