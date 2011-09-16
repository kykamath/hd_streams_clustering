'''
Created on Sep 14, 2011

@author: kykamath
'''
import sys, os, time
sys.path.append('../')
from library.file_io import FileIO
import matplotlib.pyplot as plt
from hd_streams_clustering import HDStreaminClustering
import numpy as np
from settings import default_experts_twitter_stream_settings, experts_twitter_stream_settings
from library.clustering import EvaluationMetrics
from twitter_streams_clustering import TwitterIterators, getExperts,\
    TwitterCrowdsSpecificMethods
from library.nlp import getPhrases, getWordsFromRawEnglishMessage
from algorithms_performance import emptyClusterAnalysisMethod
from settings import Settings

OPTIMIZED_ID = 'optimized'
UN_OPTIMIZED_ID = 'un_optimized'

clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'

experts_twitter_stream_settings['status_file'] = clustering_quality_experts_folder+'optimized_stats_file'
default_experts_twitter_stream_settings['status_file'] = clustering_quality_experts_folder+'unoptomized_stats_file'

def iterateData():
#    for nonOptimzed, optimized in zip(FileIO.iterateJsonFromFile(TweetsFile.default_stats_file), FileIO.iterateJsonFromFile(TweetsFile.stats_file)): yield nonOptimzed, optimized
    for nonOptimzed, optimized in zip(FileIO.iterateJsonFromFile('default_stats_file'), FileIO.iterateJsonFromFile('quality_stats')): yield nonOptimzed, optimized

class TweetsFile:
    def __init__(self, length, forGeneration=False, **stream_settings):
        self.length=length
        self.stream_settings = stream_settings
        self.stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
        self.stream_settings['cluster_analysis_method'] = emptyClusterAnalysisMethod
        self.fileName = clustering_quality_experts_folder+'data/'+str(length)
        self.expertsToClassMap = dict([(k, v['class']) for k,v in getExperts(byScreenName=True).iteritems()])
    def _getExpertClasses(self, cluster): return [self.expertsToClassMap[user.lower()] for user in cluster if user.lower() in self.expertsToClassMap]
    def getEvaluationMetrics(self, documentClusters, timeDifference):
        iterationData =  {'no_of_documents':self.length, 'no_of_clusters': len(documentClusters), 'iteration_time': timeDifference, 'clusters': documentClusters}
        clustersForEvaluation = [self._getExpertClasses(cluster) for cluster in documentClusters]
        iterationData['nmi'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.nmi)
        iterationData['purity'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.purity)
        iterationData['f1'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.f1)
        return iterationData
    def generateStatsForStreamingLSHClustering(self):
        print 'Streaming LSH'
        clustering=HDStreaminClustering(**self.stream_settings)
        ts = time.time()
        clustering.cluster(TwitterIterators.iterateFromFile(self.fileName+'.gz'))
        te = time.time()
        documentClusters = [cluster.documentsInCluster.keys() for k, cluster in clustering.clusters.iteritems() if len(cluster.documentsInCluster.keys())>=self.stream_settings['cluster_filter_threshold']]
        return self.getEvaluationMetrics(documentClusters, te-ts)
    @staticmethod
    def generateStatsFor(streamSettings = experts_twitter_stream_settings):
        for i in [10**3, 10**4, 10**5]: 
            for j in range(1, 10):
                tf = TweetsFile(i*j, **streamSettings)
                print tf.generateStatsForStreamingLSHClustering()
                FileIO.writeToFileAsJson({OPTIMIZED_ID: tf.generateStatsForStreamingLSHClustering(), 
                                          'settings': Settings.getSerialzedObject(tf.stream_settings)}, 
                                          streamSettings['status_file'])

def plotTime():
    dataX, optTime, unOptTime = [], [], []
    for nonOptimzed, optimized in iterateData():
        dataX.append(optimized['streaming_lsh']['no_of_documents'])
        optTime.append(optimized['streaming_lsh']['iteration_time'])
        unOptTime.append(nonOptimzed['streaming_lsh']['iteration_time'])
#        print 'non-opt', nonOptimzed['streaming_lsh']['iteration_time'], nonOptimzed['streaming_lsh']['nmi'], nonOptimzed['streaming_lsh']['no_of_documents'], nonOptimzed['settings']['stream_id']
#        print 'opt', optimized['streaming_lsh']['iteration_time'], optimized['streaming_lsh']['nmi'], optimized['streaming_lsh']['no_of_documents'], optimized['settings']['stream_id']
    plt.plot(dataX, optTime, label='opt')
    plt.plot(dataX, unOptTime, label='un-opt')
    plt.legend()
    plt.savefig('plt_time.eps')

def plotQuality():
    dataX, optQuality, unOptQuality = [], [], []
    for nonOptimzed, optimized in iterateData():
        dataX.append(optimized['streaming_lsh']['no_of_documents'])
        optQuality.append(optimized['streaming_lsh']['nmi'])
        unOptQuality.append(nonOptimzed['streaming_lsh']['nmi'])
#    print 'opt', np.mean(optQuality)
#    print 'un opt', np.mean(unOptQuality)
    plt.plot(dataX, optQuality, label='opt')
    plt.plot(dataX, unOptQuality, label='un-opt')
    plt.legend()
    plt.savefig('plt_quality.eps')
if __name__ == '__main__':
#    plotQuality()
#    plotTime()
    TweetsFile.generateStatsFor(experts_twitter_stream_settings)
