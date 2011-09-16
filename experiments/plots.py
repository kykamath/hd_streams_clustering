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
from algorithms_performance import emptyClusterAnalysisMethod, emptyClusterFilteringMethod
from settings import Settings

OPTIMIZED_ID = 'optimized'
UN_OPTIMIZED_ID = 'un_optimized'

clustering_quality_hd_experiments_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_hd_experiments_folder/'
clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'
clustering_quality_experts_ssa_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_ssa_folder/'

experts_twitter_stream_settings['status_file'] = clustering_quality_hd_experiments_folder+'optimized_stats_file'
default_experts_twitter_stream_settings['status_file'] = clustering_quality_hd_experiments_folder+'unoptomized_stats_file'

class DataIterators:
    @staticmethod
    def kmeans(): 
        for data in FileIO.iterateJsonFromFile(clustering_quality_experts_folder+'quality_stats'): yield data['k_means']
    @staticmethod
    def kmeansmr(): 
        for data in FileIO.iterateJsonFromFile(clustering_quality_experts_folder+'mr_quality_stats'): yield data['mr_k_means']
    @staticmethod
    def cdait(): 
        for data in FileIO.iterateJsonFromFile(clustering_quality_experts_ssa_folder+'quality_stats'): yield data['ssa']
    @staticmethod
    def cdamr(): 
        for data in FileIO.iterateJsonFromFile(clustering_quality_experts_ssa_folder+'quality_stats'): yield data['ssa_mr']
    @staticmethod
    def optimized(): 
        for data in FileIO.iterateJsonFromFile(clustering_quality_experts_ssa_folder+'quality_stats'): yield data['streaming_lsh']
    @staticmethod
    def unoptimized(): pass

def iterateData():
    for nonOptimzed, optimized in zip(FileIO.iterateJsonFromFile(TweetsFile.default_stats_file), FileIO.iterateJsonFromFile(TweetsFile.stats_file)): yield nonOptimzed, optimized
#    for nonOptimzed, optimized in zip(FileIO.iterateJsonFromFile('default_stats_file'), FileIO.iterateJsonFromFile('quality_stats')): yield nonOptimzed, optimized

class TweetsFile:
    def __init__(self, length, forGeneration=False, **stream_settings):
        self.length=length
        self.stream_settings = stream_settings
        self.stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
        self.stream_settings['cluster_analysis_method'] = emptyClusterAnalysisMethod
        self.stream_settings['cluster_filtering_method'] = emptyClusterFilteringMethod
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
        def getDocuments():
            documents = []
            for data in TwitterIterators.iterateFromFile(self.fileName+'.gz'): documents.append(TwitterCrowdsSpecificMethods.convertTweetJSONToMessage(data, self.stream_settings))
            return documents
        documents = getDocuments()
        clustering=HDStreaminClustering(**self.stream_settings)
        ts = time.time()
        clustering.cluster(documents)
        te = time.time()
        documentClusters = [cluster.documentsInCluster.keys() for k, cluster in clustering.clusters.iteritems() if len(cluster.documentsInCluster.keys())>=self.stream_settings['cluster_filter_threshold']]
        return self.getEvaluationMetrics(documentClusters, te-ts)
    @staticmethod
    def generateStatsFor(streamSettings):
        for i in [10**3, 10**4, 10**5]: 
            for j in range(1, 10):
                print 'Exerpiments for:', i*j
                tf = TweetsFile(i*j, **streamSettings)
                iteration_data = tf.generateStatsForStreamingLSHClustering()
                FileIO.writeToFileAsJson({'iteration_data': iteration_data, 
                                          'settings': Settings.getSerialzedObject(tf.stream_settings)}, 
                                          streamSettings['status_file'])
                print iteration_data

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
#    TweetsFile.generateStatsFor(default_experts_twitter_stream_settings)
#    for d in DataIterators.optimized(): 
#        del d['clusters']
#        print d
