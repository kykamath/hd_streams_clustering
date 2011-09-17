'''
Created on Jul 12, 2011

@author: kykamath
'''
import sys, os, time
from library.twitter import getDateTimeObjectFromTweetTimestamp
sys.path.append('../')
from experiments.algorithms_performance import emptyClusterAnalysisMethod,\
    emptyClusterFilteringMethod
from hd_streams_clustering import HDStreaminClustering
from classes import Stream, Message
from twitter_streams_clustering import TwitterIterators, getExperts,\
    TwitterCrowdsSpecificMethods
from library.mr_algorithms.kmeans import KMeans
from library.clustering import KMeansClustering, EvaluationMetrics, Clustering
from library.vector import Vector
from streaming_lsh.classes import Document
from streaming_lsh.streaming_lsh_clustering import StreamingLSHClustering
from library.classes import Settings
from library.plotting import getLatexForString
from library.file_io import FileIO
from library.nlp import getWordsFromRawEnglishMessage, getPhrases
from settings import experts_twitter_stream_settings, default_experts_twitter_stream_settings
from itertools import groupby
from operator import itemgetter
from matplotlib import pyplot as plt
from collections import defaultdict
import numpy as np

clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'
clustering_quality_experts_mr_folder = clustering_quality_experts_folder+'mr_data/'
hdfsPath='hdfs:///user/kykamath/lsh_experts_data/'
unique_string = ':ilab:'

experts_twitter_stream_settings['min_phrase_length'] = 1
experts_twitter_stream_settings['max_phrase_length'] = 1
experts_twitter_stream_settings['threshold_for_document_to_be_in_cluster'] = 0.5

default_experts_twitter_stream_settings['min_phrase_length'] = 1
default_experts_twitter_stream_settings['max_phrase_length'] = 1
default_experts_twitter_stream_settings['threshold_for_document_to_be_in_cluster'] = 0.5

#plotSettings = {
#                 'k_means':{'label': 'k-Means', 'color': '#FF1800'}, 
#                 'streaming_lsh': {'label': 'Streaming-LSH', 'color': '#00C322'},
#                 'mr_kmeans': {'label': 'MR k-Means', 'color': '#00C322'}
#                 }

plotSettings = {
                 'k_means':{'label': 'k-Means', 'color': '#FD0006'}, 
                 'mr_k_means': {'label': 'MR k-Means', 'color': '#5AF522'},
                 'streaming_lsh': {'label': 'Streaming-LSH', 'color': '#7109AA'},
                 'default_streaming_lsh': {'label': 'Default-Streaming-LSH', 'color': '#8299FF'},
                 }

def extractArraysFromFile(file, percentage=1.0):
    arraysToReturn = []
    for line in FileIO.iterateJsonFromFile(file): arraysToReturn.append(np.array(line['vector']))
    print len(arraysToReturn[:int(len(arraysToReturn)*percentage)])
    return arraysToReturn[:int(len(arraysToReturn)*percentage)]

class TweetsFile:
    stats_file = clustering_quality_experts_folder+'quality_stats'
    mr_stats_file = clustering_quality_experts_folder+'mr_quality_stats'
    combined_stats_file = clustering_quality_experts_folder+'combined_stats_file'
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
    def _tweetWithTimestampIterator(self):
            userMap = defaultdict(dict)
            for tweet in TwitterIterators.iterateFromFile(self.fileName+'.gz'):
                user = tweet['user']['screen_name']
                userMap[user]['user'] = {'screen_name': user}
                userMap[user]['id'] = tweet['id']
                userMap[user]['created_at'] = tweet['created_at']
                if 'text' not in userMap[user]: userMap[user]['text'] = ' '
                phrases = [phrase.replace(' ', unique_string) for phrase in getPhrases(getWordsFromRawEnglishMessage(tweet['text']), self.stream_settings['min_phrase_length'], self.stream_settings['max_phrase_length'])]
                if phrases: userMap[user]['text']+= ' ' + ' '.join(phrases)
            return userMap.iteritems()
    def _getExpertClasses(self, cluster): return [self.expertsToClassMap[user.lower()] for user in cluster if user.lower() in self.expertsToClassMap]
    def getEvaluationMetrics(self, documentClusters, timeDifference):
        iterationData =  {'no_of_documents':self.length, 'no_of_clusters': len(documentClusters), 'iteration_time': timeDifference, 'clusters': documentClusters}
        clustersForEvaluation = [self._getExpertClasses(cluster) for cluster in documentClusters]
        iterationData['nmi'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.nmi)
        iterationData['purity'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.purity)
        iterationData['f1'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.f1)
        return iterationData
    def generateStatsForKMeansClustering(self, **kwargs):
        ts = time.time()
        clusters = KMeansClustering(self.documents,len(self.documents)).cluster(**kwargs)
        te = time.time()
        documentClusters = []
        for a in [ (k, list(set(list(v)))) for k,v in groupby(sorted((a[0], a[1][0]) for a in zip(clusters, self.documents)), key=itemgetter(0))]:
            if len(a[1]) >= self.stream_settings['cluster_filter_threshold']: documentClusters.append(zip(*a[1])[1])
        return self.getEvaluationMetrics(documentClusters, te-ts)
    def generateStatsForStreamingLSHClustering(self):
        print 'Streaming LSH'
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
        return self.getEvaluationMetrics(documentClusters, te-ts)
    def generateStatsForHDLSHClustering(self):
        print 'HD LSH'
        def _getDocumentFromTuple((user, text)):
            vector, words = Vector(), text.split()
            for word in words[1:]:
                if word not in vector: vector[word]=1
                else: vector[word]+=1
            return Document(user, vector)
        self.stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
        self.stream_settings['cluster_analysis_method'] = emptyClusterAnalysisMethod
        self.stream_settings['cluster_filtering_method'] = emptyClusterFilteringMethod
        self.documents = [tw[1] for tw in list(self._tweetWithTimestampIterator()) if tw[1]['text'].strip()!='']
        self.documents = [ tw[0] for tw in 
                          sorted([(t, getDateTimeObjectFromTweetTimestamp(t['created_at']))  for t in self.documents], key=itemgetter(0))
                          ]
        clustering=HDStreaminClustering(**self.stream_settings)
        ts = time.time()
#        for tweet in self.documents: clustering.getClusterAndUpdateExistingClusters(_getDocumentFromTuple(tweet))
#        clustering.cluster([_getDocumentFromTuple(d) for d in self.documents])
        clustering.cluster(self.documents)
        te = time.time()
        documentClusters = [cluster.documentsInCluster.keys() for k, cluster in clustering.clusters.iteritems() if len(cluster.documentsInCluster.keys())>=self.stream_settings['cluster_filter_threshold']]
        return self.getEvaluationMetrics(documentClusters, te-ts)
    def generateStatsForKMeansMRClustering(self):
        ts = time.time()
        documentClusters = list(KMeans.cluster(hdfsPath+'%s'%self.length, 
                                               extractArraysFromFile(clustering_quality_experts_mr_folder+'%s'%self.length, 0.5), 
                                               mrArgs='-r hadoop', iterations=1, 
                                               jobconf={'mapred.map.tasks':25, 'mapred.task.timeout': 7200000}))
        documentClusters = [cluster for cluster in documentClusters if len(cluster)>=self.stream_settings['cluster_filter_threshold']]
        te = time.time()
        return self.getEvaluationMetrics(documentClusters, te-ts)
    def generate(self):
        i=0
        for tweet in TwitterIterators.iterateTweetsFromExperts(): 
            FileIO.writeToFileAsJson(tweet, self.fileName)
            i+=1
            if i==self.length: break;
        os.system('gzip %s'%self.fileName)
    @staticmethod
    def generateDocumentForMRClustering():
        for i in [10**3, 10**4, 10**5]: 
            for j in range(1, 10): 
                print 'Generating file for: ',i*j
                tf = TweetsFile(i*j, **experts_twitter_stream_settings)
                outputFile = clustering_quality_experts_mr_folder+tf.fileName.split('/')[-1]
                Clustering(tf.documents,len(tf.documents)).dumpDocumentVectorsToFile(outputFile)
    @staticmethod
    def generateStatsForClusteringQuality():
        for i in [10**3, 10**4, 10**5]: 
            for j in range(1, 10): 
                print 'Generating stats for: ',i*j
                tf = TweetsFile(i*j, **experts_twitter_stream_settings)
                FileIO.writeToFileAsJson({'k_means': tf.generateStatsForKMeansClustering(), 
                                          'streaming_lsh': tf.generateStatsForStreamingLSHClustering(), 
                                          'settings': Settings.getSerialzedObject(tf.stream_settings)}, 
                                          TweetsFile.stats_file)
    @staticmethod
    def generateStatsForOptimized():
        for i in [10**3, 10**4, 10**5]: 
            for j in range(1, 10): 
                print 'Generating stats for: ',i*j
                tf = TweetsFile(i*j, **experts_twitter_stream_settings)
                print tf.generateStatsForHDLSHClustering()
#                FileIO.writeToFileAsJson({'streaming_lsh': tf.generateStatsForHDLSHClustering(), 
#                                          'settings': Settings.getSerialzedObject(tf.stream_settings)}, 
#                                          TweetsFile.stats_file)
    @staticmethod
    def generateStatsForMRKMeansClusteringQuality():
        for i in [90000, 100000, 200000, 300000, 400000, 500000]: 
            print 'Generating stats for: ',i
            tf = TweetsFile(i, **experts_twitter_stream_settings)
            FileIO.writeToFileAsJson({'mr_k_means': tf.generateStatsForKMeansMRClustering(), 
                                      'settings': Settings.getSerialzedObject(tf.stream_settings)}, 
                                      TweetsFile.mr_stats_file)
    @staticmethod
    def generateStatsForDefaultStreamSettings():
        for i in [10**3, 10**4, 10**5]: 
            for j in range(1, 10):
                print 'Generating stats for: ',i*j
                tf = TweetsFile(i*j, **default_experts_twitter_stream_settings)
                FileIO.writeToFileAsJson({'streaming_lsh': tf.generateStatsForStreamingLSHClustering(), 
                                          'settings': Settings.getSerialzedObject(tf.stream_settings)}, 
                                          TweetsFile.default_stats_file)
    @staticmethod
    def plotClusteringSpeed(saveFig=True):
        dataToPlot = {'k_means': {'x': [], 'y': []}, 'mr_k_means': {'x': [], 'y': []}, 'streaming_lsh': {'x': [], 'y': []}}
        for data in FileIO.iterateJsonFromFile(TweetsFile.combined_stats_file):
            for k in plotSettings: dataToPlot[k]['x'].append(data[k]['no_of_documents']); dataToPlot[k]['y'].append(data[k]['iteration_time'])
        for k in plotSettings: plt.loglog(dataToPlot[k]['x'], dataToPlot[k]['y'], label=plotSettings[k]['label'], color=plotSettings[k]['color'], lw=2)
        plt.legend(loc=4); 
        plt.xlabel(getLatexForString('\# of documents')); plt.ylabel(getLatexForString('Running time (s)')); plt.title(getLatexForString('Running time comparsion for Streaing LSH with k-Means'))
        plt.xlim(xmax=500000)
#        plt.show()
        if saveFig: plt.savefig('speedComparisonWithKMeans.pdf')
    @staticmethod
    def getClusteringQuality():
        '''
        no_of_documents: 300000
        k_means
            f1, p, r ['(0.95 0.04)', '(0.95 0.04)', '(0.95 0.04)']
            purity (0.95 0.04)
            nmi (0.94 0.04)
        streaming_lsh
            f1, p, r ['(0.67 0.01)', '(0.71 0.01)', '(0.64 0.02)']
            purity (0.96 0.00)
            nmi (0.87 0.00)
        '''
        del plotSettings['mr_k_means']
        speedStats = dict([(k, {'f1': [], 'nmi': [], 'purity': []}) for k in plotSettings])
        for data in FileIO.iterateJsonFromFile(TweetsFile.combined_stats_file):
            for k in speedStats:
                for metric in speedStats['k_means']: speedStats[k][metric].append(data[k][metric])
        # Adding this because final value of f1 is 0 instead of tuple at 300K documents.
        speedStats['k_means']['f1'][-1]=[0.,0.,0.]
        dataForPlot = dict([(k, []) for k in plotSettings])
        for k, v in speedStats.iteritems(): 
            print k
            for k1,v1 in v.iteritems(): 
                if type(v1[0])!=type([]): print k1, '(%0.2f %0.2f)'%(np.mean(v1), np.var(v1)); dataForPlot[k]+=[np.mean(v1)]
                else: print k1, ['(%0.2f %0.2f)'%(np.mean(z),np.var(z)) for z in zip(*v1)]; dataForPlot[k]+=[np.mean(z) for z in zip(*v1)]
        ind, width = np.arange(5), 0.1
        rects, i = [], 0
        for k in dataForPlot: 
            rects.append(plt.bar(ind+i*width, dataForPlot[k], width, color=plotSettings[k]['color']))
            i+=1
        plt.ylabel(getLatexForString('Score'))
        plt.title(getLatexForString('Clustering quality comparison for Streaming LSH with k-Means'))
        plt.xticks(ind+width, ('$F$', '$Precision$', '$Recall$', '$Purity$', '$NMI$') )
        plt.legend( [r[0] for r in rects], [plotSettings[k]['label'] for k in plotSettings], loc=4 )
#        plt.show()
        plt.savefig('qualityComparisonWithKMeans.pdf')
    @staticmethod
    def generateCombinedStatsFile():
        for normalData, mrData in zip(FileIO.iterateJsonFromFile(TweetsFile.stats_file), FileIO.iterateJsonFromFile(TweetsFile.mr_stats_file)):
            normalData['mr_k_means'] = mrData['mr_k_means']
            FileIO.writeToFileAsJson(normalData, TweetsFile.combined_stats_file)
                
if __name__ == '__main__':
#    [TweetsFile(i*j, forGeneration=True, **experts_twitter_stream_settings).generate() for i in [10**2] for j in range(1, 10)]
#    TweetsFile.generateStatsForClusteringQuality()
    TweetsFile.generateStatsForOptimized()
#    TweetsFile.generateStatsForMRKMeansClusteringQuality()
#    TweetsFile.generateDocumentForMRClustering()
#    TweetsFile.generateStatsForDefaultStreamSettings()
#    TweetsFile.plotClusteringSpeed()
#    TweetsFile.getClusteringQuality()
#    TweetsFile.generateDocumentForMRClustering()
#    TweetsFile.generateCombinedStatsFile()
    
    
    
