'''
Created on Sep 30, 2011

@author: kykamath
'''
from library.twitter import TweetFiles
from library.file_io import FileIO
from settings import experts_twitter_stream_settings
from twitter_streams_clustering import TwitterCrowdsSpecificMethods
from hd_streams_clustering import HDStreaminClustering
from experiments.algorithms_performance import Evaluation
import time

time_to_process_points = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/time_to_process_points/'
experts_twitter_stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
experts_twitter_stream_settings['min_phrase_length'] = 1
experts_twitter_stream_settings['max_phrase_length'] = 1
experts_twitter_stream_settings['threshold_for_document_to_be_in_cluster'] = 0.5

previousTime = None
evaluation = Evaluation()

def generateData():
    i = 0
    for line in TweetFiles.iterateTweetsFromGzip('/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/data/1000000.gz'):
        FileIO.writeToFileAsJson(line, time_to_process_points+'%s'%(i/50000))
        i+=1
        
def fileIterator(): 
    for id in xrange(20): yield FileIO.iterateJsonFromFile(time_to_process_points+'%s'%id)
    
def clusterAnalysis(hdStreamClusteringObject, currentMessageTime):
    global evaluation, previousTime
    currentTime = time.time()
#    documentClusters = [cluster.documentsInCluster.keys() for k, cluster in hdStreamClusteringObject.clusters.iteritems() if len(cluster.documentsInCluster.keys())>=experts_twitter_stream_settings['cluster_filter_threshold']]
#    iteration_data = evaluation.getEvaluationMetrics(documentClusters, currentTime-previousTime)
    print ' ******************* ', currentTime-previousTime
    previousTime = time.time()
    time.sleep(5000)
#    print iteration_data
#    FileIO.writeToFileAsJson(iteration_data, JustifyDimensionsEstimation.stats_file)
#    del iteration_data['clusters']
#    print currentMessageTime, iteration_data
#    if experts_twitter_stream_settings['dimensions']!=76819 and 2*experts_twitter_stream_settings['dimensions']<=len(hdStreamClusteringObject.phraseTextToPhraseObjectMap): raise Exception

def getStatsForCDA():
    previousTime = time.time()
    experts_twitter_stream_settings['cluster_analysis_method'] = clusterAnalysis
    HDStreaminClustering(**experts_twitter_stream_settings).cluster(TweetFiles.iterateTweetsFromGzip('/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/data/1000000.gz')) 

#generateData()

#for l in fileIterator(): print l
getStatsForCDA()