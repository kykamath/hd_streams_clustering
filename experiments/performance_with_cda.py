'''
Created on Sep 16, 2011

@author: kykamath
'''
import sys, time
sys.path.append('../')
from experiments.ssa.ssa import SimilarStreamAggregation
from collections import defaultdict
from library.vector import Vector
from library.twitter import TweetFiles
from twitter_streams_clustering import TwitterCrowdsSpecificMethods,\
    Evaluation, TwitterIterators
from settings import experts_twitter_stream_settings
from library.classes import Settings
from library.file_io import FileIO
from hd_streams_clustering import HDStreaminClustering
from experiments.algorithms_performance import emptyClusterAnalysisMethod,\
    emptyClusterFilteringMethod
import matplotlib.pyplot as plt

experts_twitter_stream_settings['ssa_threshold']=0.75
experts_twitter_stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
experts_twitter_stream_settings['cluster_analysis_method'] = emptyClusterAnalysisMethod
experts_twitter_stream_settings['cluster_filtering_method'] = emptyClusterFilteringMethod

clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'
hd_clustering_performance_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/hd_clustering_performance/'

CDA_IT = 'cda_it'
CDA = 'cda'

algorithm_info = {
                  CDA: {'label': 'CDA', 'color': 'k'},
                  CDA_IT: {'label': 'CDA-IT', 'color': 'b'}
                  }

def iterateTweetUsersAfterCombiningTweets(fileName, **stream_settings):
        dataForAggregation = defaultdict(Vector)
        textToIdMap = defaultdict(int)
        for tweet in TweetFiles.iterateTweetsFromGzip(fileName):
            textVector = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage(tweet, **stream_settings).vector
            textIdVector = Vector()
            for phrase in textVector: 
                if phrase not in textToIdMap: textToIdMap[phrase]=str(len(textToIdMap))
                textIdVector[textToIdMap[phrase]]=textVector[phrase]
            dataForAggregation[tweet['user']['screen_name'].lower()]+=textIdVector
        for k, v in dataForAggregation.iteritems(): yield k, v
        
def getPerformanceFile(algorithmId): return hd_clustering_performance_folder+algorithmId
def iteratePerformanceFrom(id): 
    for data in FileIO.iterateJsonFromFile(getPerformanceFile(id)): 
        del data[id]['clusters']
        yield data[id]
        
class GenerateStats():
    @staticmethod
    def lengthAndFileIterator(): return ((i*j, clustering_quality_experts_folder+'data/%s.gz'%str(i*j)) for i in [10**3, 10**4, 10**5] for j in range(1, 10))
    @staticmethod
    def performanceForCDAITAt(noOfTweets, fileName, **stream_settings):
        ts = time.time()
        sstObject = SimilarStreamAggregation(dict(iterateTweetUsersAfterCombiningTweets(fileName, **stream_settings)), stream_settings['ssa_threshold'])
        sstObject.estimate()
        documentClusters = list(sstObject.iterateClusters())
        te = time.time()
        return Evaluation.getEvaluationMetrics(noOfTweets, documentClusters, te-ts)
    @staticmethod
    def performanceForCDAAt(noOfTweets, fileName, **stream_settings):
        clustering=HDStreaminClustering(**stream_settings)
        ts = time.time()
        clustering.cluster(TwitterIterators.iterateFromFile(fileName))
        te = time.time()
        documentClusters = [cluster.documentsInCluster.keys() for k, cluster in clustering.clusters.iteritems() if len(cluster.documentsInCluster.keys())>=stream_settings['cluster_filter_threshold']]
        return Evaluation.getEvaluationMetrics(noOfTweets, documentClusters, te-ts)
    @staticmethod
    def generateStatsForCDAIT():
        for length, fileName in GenerateStats.lengthAndFileIterator(): 
            print 'Generating stats for: ',length
            performance = GenerateStats.performanceForCDAITAt(length, fileName, **experts_twitter_stream_settings)
            stats = {CDA_IT: performance, 'settings': Settings.getSerialzedObject(experts_twitter_stream_settings)}
            FileIO.writeToFileAsJson(stats, getPerformanceFile(CDA_IT))
    @staticmethod
    def generateStatsForCDA():
        for length, fileName in GenerateStats.lengthAndFileIterator(): 
            print 'Generating stats for: ',length
            performance = GenerateStats.performanceForCDAAt(length, fileName, **experts_twitter_stream_settings)
            stats = {CDA: performance, 'settings': Settings.getSerialzedObject(experts_twitter_stream_settings)}
            FileIO.writeToFileAsJson(stats, getPerformanceFile(CDA))
            
class CompareAlgorithms:
    @staticmethod
    def runningTimes(*algorithmIds):
        for id in algorithmIds:
            dataX, dataY = [], []
            for data in iteratePerformanceFrom(id): dataX.append(data['no_of_tweets']), dataY.append(data['iteration_time'])
            plt.plot(dataX, dataY, label=algorithm_info[id]['label'], color=algorithm_info[id]['color'])
        plt.legend()
        plt.show()
    
if __name__ == '__main__':
#    GenerateStats.generateStatsForCDAIT()
#    GenerateStats.generateStatsForCDA()
    
#    for d in iteratePerformanceFrom(CDA):
#        print d
    CompareAlgorithms.runningTimes(CDA, CDA_IT)