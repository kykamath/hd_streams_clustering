'''
Created on Sep 30, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from library.twitter import TweetFiles
from library.file_io import FileIO
from settings import default_experts_twitter_stream_settings
from twitter_streams_clustering import TwitterCrowdsSpecificMethods
from hd_streams_clustering import HDStreaminClustering,\
    HDSkipStreamClustering
from experiments.algorithms_performance import Evaluation
import time

time_to_process_points = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/time_to_process_points/'
default_experts_twitter_stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
default_experts_twitter_stream_settings['min_phrase_length'] = 1
default_experts_twitter_stream_settings['max_phrase_length'] = 1
default_experts_twitter_stream_settings['threshold_for_document_to_be_in_cluster'] = 0.5

previousTime = None
evaluation = Evaluation()

stream_cda_stats_file = time_to_process_points+'stats/stream_cda'

def generateData():
    i = 0
    for line in TweetFiles.iterateTweetsFromGzip('/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/data/1000000.gz'):
        FileIO.writeToFileAsJson(line, time_to_process_points+'%s'%(i/50000))
        i+=1
        
def fileIterator(): 
    for id in xrange(20): yield FileIO.iterateJsonFromFile(time_to_process_points+'%s'%id)
    
#def clusterAnalysis(hdStreamClusteringObject, currentMessageTime, numberOfMessages):
#    global evaluation, previousTime
#    iteration_data = {'iteration_time': time.time()-previousTime, 'type': 'stream-cda', 'number_of_messages': numberOfMessages}
#    previousTime = time.time()
#    print iteration_data
#    FileIO.writeToFileAsJson(iteration_data, stream_cda_stats_file)

def clusterAnalysis(hdStreamClusteringObject, currentMessageTime, numberOfMessages):
    global evaluation, previousTime
    iteration_data = {'iteration_time': time.time()-previousTime, 'type': 'stream-cda', 'number_of_messages': numberOfMessages}
#    previousTime = time.time()
    print iteration_data

def getStatsForCDA():
    global previousTime
    default_experts_twitter_stream_settings['cluster_analysis_method'] = clusterAnalysis
    clustering = HDSkipStreamClustering(**default_experts_twitter_stream_settings)
    previousTime = time.time()
    clustering.cluster(TweetFiles.iterateTweetsFromGzip('/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/data/1000000.gz')) 

#generateData()

#for l in fileIterator(): print l
getStatsForCDA()

#clustering = HDDelayedClustering(**default_experts_twitter_stream_settings)
#clustering.cluster(TweetFiles.iterateTweetsFromGzip('/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/data/1000000.gz')) 