'''
Created on Sep 16, 2011

@author: kykamath
'''
import sys, time
from library.classes import Settings
from library.file_io import FileIO
sys.path.append('../')
from experiments.ssa.ssa import SimilarStreamAggregation
from collections import defaultdict
from library.vector import Vector
from library.twitter import TweetFiles
from twitter_streams_clustering import TwitterCrowdsSpecificMethods,\
    Evaluation
from settings import experts_twitter_stream_settings

experts_twitter_stream_settings['ssa_threshold']=0.75

clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'
hd_clustering_performance_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/hd_clustering_performance/'

CDA_IT = 'cda_it'
CDA_IT_PERFORMANCE_FILE = hd_clustering_performance_folder+CDA_IT

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
        
class GenerateStats():
    @staticmethod
    def lengthAndFileIterator(): return ((i*j, clustering_quality_experts_folder+'data/%s.gz'%str(i*j)) for i in [10**3, 10**4, 10**5] for j in range(1, 10))
    @staticmethod
    def performanceForCDAITAt(noOfTweets, fileName, **stream_settings):
        inputDataFileName = clustering_quality_experts_folder+'data/%s.gz'%str(noOfTweets)
        ts = time.time()
        sstObject = SimilarStreamAggregation(dict(iterateTweetUsersAfterCombiningTweets(inputDataFileName, **stream_settings)), stream_settings['ssa_threshold'])
        sstObject.estimate()
        documentClusters = list(sstObject.iterateClusters())
        te = time.time()
        return Evaluation.getEvaluationMetrics(noOfTweets, documentClusters, te-ts)
    @staticmethod
    def performanceForCDA(noOfTweets, **stream_settings):
        inputDataFileName = clustering_quality_experts_folder+'data/%s.gz'%str(noOfTweets)
    @staticmethod
    def generateStatsForCDAIT():
        for length, fileName in GenerateStats.lengthAndFileIterator(): 
            print 'Generating stats for: ',length
            performance = GenerateStats.performanceForCDAITAt(length, fileName, **experts_twitter_stream_settings)
            print performance
#            stats = {CDA_IT: performance, 'settings': Settings.getSerialzedObject(experts_twitter_stream_settings)}
#            FileIO.writeToFileAsJson(stats, CDA_IT_PERFORMANCE_FILE)
    
if __name__ == '__main__':
    GenerateStats.generateStatsForCDAIT()