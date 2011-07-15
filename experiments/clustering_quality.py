'''
Created on Jul 12, 2011

@author: kykamath
'''
import sys, os, time
from library.mr_algorithms.kmeans import KMeans
sys.path.append('../')
from library.clustering import KMeansClustering, EvaluationMetrics, Clustering
from library.vector import Vector
from streaming_lsh.classes import Document
from streaming_lsh.streaming_lsh_clustering import StreamingLSHClustering
from library.classes import Settings
from library.plotting import getLatexForString
from twitter_streams_clustering import TwitterIterators, getExperts
from library.file_io import FileIO
from library.nlp import getWordsFromRawEnglishMessage, getPhrases
from settings import experts_twitter_stream_settings
from itertools import groupby
from operator import itemgetter
from matplotlib import pyplot as plt
import numpy as np

clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'
clustering_quality_experts_mr_folder = clustering_quality_experts_folder+'mr_data/'
unique_string = ':ilab:'

experts_twitter_stream_settings['min_phrase_length'] = 1
experts_twitter_stream_settings['max_phrase_length'] = 1
experts_twitter_stream_settings['threshold_for_document_to_be_in_cluster'] = 0.5

plotSettings = {
                 'k_means':{'label': 'k-Means', 'color': '#FF1800'}, 
                 'streaming_lsh': {'label': 'Streaming-LSH', 'color': '#00C322'}
                 }

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
    def plotClusteringSpeed():
        dataToPlot = {'k_means': {'x': [], 'y': []}, 'streaming_lsh': {'x': [], 'y': []}}
        for data in FileIO.iterateJsonFromFile(TweetsFile.stats_file):
            for k in plotSettings: dataToPlot[k]['x'].append(data[k]['no_of_documents']); dataToPlot[k]['y'].append(data[k]['iteration_time'])
        for k in plotSettings: plt.loglog(dataToPlot[k]['x'], dataToPlot[k]['y'], label=plotSettings[k]['label'], color=plotSettings[k]['color'], lw=2)
        plt.legend(loc=4); 
        plt.xlabel(getLatexForString('\# of documents')); plt.ylabel(getLatexForString('Running time (s)')); plt.title(getLatexForString('Running time comparsion for Streaing LSH'))
        plt.show()
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
        speedStats = {'k_means': {'f1': [], 'nmi': [], 'purity': []}, 'streaming_lsh': {'f1': [], 'nmi': [], 'purity': []} }
        for data in FileIO.iterateJsonFromFile(TweetsFile.stats_file):
            for k in speedStats:
                for metric in speedStats['k_means']: speedStats[k][metric].append(data[k][metric])
        # Adding this because final value of f1 is 0 instead of tuple at 300K documents.
        speedStats['k_means']['f1'][-1]=[0.,0.,0.]
        dataForPlot = {'k_means': [], 'streaming_lsh': []}
        for k, v in speedStats.iteritems(): 
            print k
            for k1,v1 in v.iteritems(): 
                if type(v1[0])!=type([]): print k1, '(%0.2f %0.2f)'%(np.mean(v1), np.var(v1)); dataForPlot[k]+=[np.mean(v1)]
                else: print k1, ['(%0.2f %0.2f)'%(np.mean(z),np.var(z)) for z in zip(*v1)]; dataForPlot[k]+=[np.mean(z) for z in zip(*v1)]
        ind, width = np.arange(5), 0.1
        rects1 = plt.bar(ind, dataForPlot['k_means'], width, color=plotSettings['k_means']['color'])
        rects2 = plt.bar(ind+width, dataForPlot['streaming_lsh'], width, color=plotSettings['streaming_lsh']['color'])
        plt.ylabel(getLatexForString('Score'))
        plt.title(getLatexForString('Clustering quality comparison for Streaming LSH'))
        plt.xticks(ind+width, ('$F$', '$Precision$', '$Recall$', '$Purity$', '$NMI$') )
        plt.legend( (rects1[0], rects2[0]), (plotSettings[plotSettings.keys()[0]]['label'], plotSettings[plotSettings.keys()[1]]['label']), loc=4 )
        plt.show()
                
if __name__ == '__main__':
#    [TweetsFile(i*j, forGeneration=True, **experts_twitter_stream_settings).generate() for i in [10**2] for j in range(1, 10)]
#    TweetsFile.generateStatsForClusteringQuality()
#    TweetsFile.plotClusteringSpeed()
#    TweetsFile.getClusteringQuality()
#    TweetsFile.generateDocumentForMRClustering()
    def extractArraysFromFile(file, numberOfArrays=float('+inf')):
        arraysToReturn = []
        for line in FileIO.iterateJsonFromFile(file):
            if numberOfArrays==0: break;
            else: numberOfArrays-=1
            arraysToReturn.append(np.array(line['vector']))
        return arraysToReturn
    fileName = clustering_quality_experts_mr_folder+'100'
#    print fileName
    print list(KMeans.cluster(fileName, extractArraysFromFile(fileName), mrArgs='-r hadoop', iterations=5))
    
