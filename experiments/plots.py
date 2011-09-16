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
from settings import default_experts_twitter_stream_settings
from library.clustering import EvaluationMetrics
from twitter_streams_clustering import TwitterIterators, getExperts,\
    TwitterCrowdsSpecificMethods
from library.nlp import getPhrases, getWordsFromRawEnglishMessage

clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'
unique_string = ':ilab:'

def iterateData():
#    for nonOptimzed, optimized in zip(FileIO.iterateJsonFromFile(TweetsFile.default_stats_file), FileIO.iterateJsonFromFile(TweetsFile.stats_file)): yield nonOptimzed, optimized
    for nonOptimzed, optimized in zip(FileIO.iterateJsonFromFile('default_stats_file'), FileIO.iterateJsonFromFile('quality_stats')): yield nonOptimzed, optimized

default_experts_twitter_stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage

class TweetsFile:
    stats_file = clustering_quality_experts_folder+'quality_stats'
    default_stats_file = clustering_quality_experts_folder+'default_stats_file'
    def __init__(self, length, forGeneration=False, **stream_settings):
        self.length=length
        self.stream_settings = stream_settings
        self.fileName = clustering_quality_experts_folder+'data/'+str(length)
        self.expertsToClassMap = dict([(k, v['class']) for k,v in getExperts(byScreenName=True).iteritems()])
        if not forGeneration: self.documents = list(self._tweetIterator())
    def _tweetIterator(self):
            userMap = {}
            for tweet in TwitterIterators.iterateFromFile(self.fileName+'.gz'):
                user = tweet['user']['screen_name']
                phrases = [phrase.replace(' ', unique_string) for phrase in getPhrases(getWordsFromRawEnglishMessage(tweet['text']), self.stream_settings['min_phrase_length'], self.stream_settings['max_phrase_length'])]
                if phrases:
                    if user not in userMap: userMap[user] = ' '.join(phrases)
                    else: userMap[user]+= ' ' + ' '.join(phrases)
            return userMap.iteritems()
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
#        def _getDocumentFromTuple((user, text)):
#            vector, words = Vector(), text.split()
#            for word in words[1:]:
#                if word not in vector: vector[word]=1
#                else: vector[word]+=1
#            return Document(user, vector)
        clustering=HDStreaminClustering(**self.stream_settings)
        ts = time.time()
#        for tweet in self.documents: clustering.getClusterAndUpdateExistingClusters(_getDocumentFromTuple(tweet))
        clustering.cluster(self.documents)
        te = time.time()
        documentClusters = [cluster.documentsInCluster.keys() for k, cluster in clustering.clusters.iteritems() if len(cluster.documentsInCluster.keys())>=self.stream_settings['cluster_filter_threshold']]
        return self.getEvaluationMetrics(documentClusters, te-ts)

    
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

    tf = TweetsFile(1000, **default_experts_twitter_stream_settings)
    for i in TwitterIterators.iterateFromFile(tf.fileName+'.gz'):
        print TwitterCrowdsSpecificMethods.convertTweetJSONToMessage(i, **default_experts_twitter_stream_settings)
#    print tf.generateStatsForStreamingLSHClustering()