'''
Created on Jul 12, 2011

@author: kykamath
'''
import sys, os, time
from library.clustering import KMeansClustering, EvaluationMetrics
from library.vector import Vector
from streaming_lsh.classes import Document
from streaming_lsh.streaming_lsh_clustering import StreamingLSHClustering
from library.classes import Settings
sys.path.append('../')
from twitter_streams_clustering import TwitterIterators, getExperts
from library.file_io import FileIO
from library.nlp import getWordsFromRawEnglishMessage, getPhrases
from settings import experts_twitter_stream_settings
from itertools import groupby
from operator import itemgetter

clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'
unique_string = ':ilab:'

experts_twitter_stream_settings['min_phrase_length'] = 1
experts_twitter_stream_settings['max_phrase_length'] = 1
experts_twitter_stream_settings['threshold_for_document_to_be_in_cluster'] = 0.5

#class GenerateData:
#    @staticmethod
#    def forLength(length):
#        i=0
#        fileName=clustering_quality_experts_data_folder+str(length)
#        for tweet in TwitterIterators.iterateTweetsFromExperts(): 
#            FileIO.writeToFileAsJson(tweet, fileName)
#            i+=1
#            if i==length: break;
#        os.system('gzip %s'%fileName)
        
class TweetsFile:
    stats_file = clustering_quality_experts_folder+'quality_stats'
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
    def getEvaluationMetrics(self, clustersForEvaluation, iterationData):
        iterationData['nmi'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.nmi)
        iterationData['purity'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.purity)
        iterationData['f1'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.f1)
    def generateStatsForKMeansClustering(self):
        ts = time.time()
        clusters = KMeansClustering(self.documents,len(self.documents)).cluster()
        te = time.time()
        documentClusters = []
        for a in [ (k, list(set(list(v)))) for k,v in groupby(sorted((a[0], a[1][0]) for a in zip(clusters, self.documents)), key=itemgetter(0))]:
            if len(a[1]) >= self.stream_settings['cluster_filter_threshold']: documentClusters.append(zip(*a[1])[1])
        iterationData =  {'no_of_documents':self.length, 'no_of_clusters': len(documentClusters), 'iteration_time': te-ts, 'clusters': documentClusters}
        self.getEvaluationMetrics([self._getExpertClasses(cluster) for cluster in documentClusters], iterationData)
        return iterationData
    def generateStatsForStreamingLSHClustering(self):
        def _getDocumentFromTuple((user, text)):
            vector, words = Vector(), text.split()
            for word in words[1:]:
                if word not in vector: vector[word]=1
                else: vector[word]+=1
            return Document(user, vector)
        clustering=StreamingLSHClustering(**self.stream_settings)
        ts = time.time()
        for tweet in self.documents: clustering.getClusterAndUpdateExistingClusters(_getDocumentFromTuple(tweet))
        te = time.time()
        documentClusters = [cluster.documentsInCluster.keys() for k, cluster in clustering.clusters.iteritems() if len(cluster.documentsInCluster.keys())>=self.stream_settings['cluster_filter_threshold']]
        iterationData =  {'no_of_documents':self.length, 'no_of_clusters': len(documentClusters), 'iteration_time': te-ts, 'clusters': documentClusters}
        self.getEvaluationMetrics([self._getExpertClasses(cluster) for cluster in documentClusters], iterationData)
        return iterationData
    def generate(self):
        i=0
        for tweet in TwitterIterators.iterateTweetsFromExperts(): 
            FileIO.writeToFileAsJson(tweet, self.fileName)
            i+=1
            if i==self.length: break;
        os.system('gzip %s'%self.fileName)
    @staticmethod
    def generateStatsForClusteringQuality():
#        for i in [10**3, 10**4, 10**5]: 
#            for j in range(1, 10): 
        i,j = 1000, 1
        print 'Generating stats for: ',i*j
        tf = TweetsFile(i*j, **experts_twitter_stream_settings)
        FileIO.writeToFileAsJson({'k_means': tf.generateStatsForKMeansClustering(), 
                                  'streaming_lsh': tf.generateStatsForStreamingLSHClustering(), 
                                  'settings': Settings.getSerialzedObject(tf.stream_settings)}, 
                                  TweetsFile.stats_file)
                
if __name__ == '__main__':
#    [TweetsFile(i*j, forGeneration=True, **experts_twitter_stream_settings).generate() for i in [10**2] for j in range(1, 10)]
    
#    tf = TweetsFile(2000, **experts_twitter_stream_settings)
#    print tf.generateStatsForKMeansClustering()
#    print tf.generateStatsForStreamingLSHClustering()
    TweetsFile.generateStatsForClusteringQuality()
