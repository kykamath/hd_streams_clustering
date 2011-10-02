'''
Created on Sep 30, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from library.vector import Vector
from experiments.ssa.ssa import SimilarStreamAggregation
from library.twitter import TweetFiles
from library.file_io import FileIO
from settings import default_experts_twitter_stream_settings
from twitter_streams_clustering import TwitterCrowdsSpecificMethods
from hd_streams_clustering import HDStreaminClustering,\
    HDSkipStreamClustering
from experiments.algorithms_performance import Evaluation
from collections import defaultdict
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
        FileIO.writeToFileAsJson(line, time_to_process_points+'10000/%s'%(i/10000))
        i+=1
        
def fileIterator(): 
    for id in xrange(20): yield FileIO.iterateJsonFromFile(time_to_process_points+'%s'%id)
    
def clusterAnalysis(hdStreamClusteringObject, currentMessageTime, numberOfMessages):
    global evaluation, previousTime
    iteration_data = {'iteration_time': time.time()-previousTime, 'type': 'stream-cda', 'number_of_messages': numberOfMessages}
#    previousTime = time.time()
    print iteration_data
    FileIO.writeToFileAsJson(iteration_data, stream_cda_stats_file)

def getStatsForCDA():
    global previousTime
    default_experts_twitter_stream_settings['cluster_analysis_method'] = clusterAnalysis
    default_experts_twitter_stream_settings['cluster_analysis_frequency_in_seconds'] = 30
    clustering = HDSkipStreamClustering(**default_experts_twitter_stream_settings)
    previousTime = time.time()
    clustering.cluster(TweetFiles.iterateTweetsFromGzip('/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/data/1000000.gz')) 
    
def iterateUserDocuments():
    dataForAggregation = defaultdict(Vector)
    textToIdMap = defaultdict(int)
    for tweet in FileIO.iterateJsonFromFile('/mnt/chevron/kykamath/data/twitter/lsh_clustering/time_to_process_points/10000/0'):
        textVector = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage(tweet, **default_experts_twitter_stream_settings).vector
        textIdVector = Vector()
        for phrase in textVector: 
            if phrase not in textToIdMap: textToIdMap[phrase]=str(len(textToIdMap))
            textIdVector[textToIdMap[phrase]]=textVector[phrase]
        dataForAggregation[tweet['user']['screen_name'].lower()]+=textIdVector
    for k, v in dataForAggregation.iteritems(): yield k, v
    
def getStatsForSSA():
    batchSize = 10000
    id = 0
    fileName = time_to_process_points+'%s/%s'%(id, batchSize)
    ts = time.time()
    sstObject = SimilarStreamAggregation(dict(iterateUserDocuments(fileName)), default_experts_twitter_stream_settings['ssa_threshold'])
    sstObject.estimate()
#    documentClusters = list(sstObject.iterateClusters())
    iteration_data = {'iteration_time': time.time()-ts, 'type': 'ssa', 'number_of_messages': batchSize*(id+1), 'batch_size': batchSize}
    print iteration_data

#getStatsForCDA()

#generateData()

#iterateUserDocuments()
getStatsForSSA()
