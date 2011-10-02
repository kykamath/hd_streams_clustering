'''
Created on Sep 30, 2011

@author: kykamath
'''
import sys, os
from library.mrjobwrapper import CJSONProtocol
from library.plotting import getLatexForString
sys.path.append('../')
from library.vector import Vector
from experiments.ssa.ssa import SimilarStreamAggregation,\
    StreamSimilarityAggregationMR
from library.twitter import TweetFiles
from library.file_io import FileIO
from settings import default_experts_twitter_stream_settings
from twitter_streams_clustering import TwitterCrowdsSpecificMethods
from hd_streams_clustering import HDStreaminClustering,\
    HDSkipStreamClustering
from experiments.algorithms_performance import Evaluation
from collections import defaultdict
from itertools import combinations
import time
import matplotlib.pyplot as plt


time_to_process_points = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/time_to_process_points/'
default_experts_twitter_stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
hdfsUnzippedPath='hdfs:///user/kykamath/lsh_experts_data/clustering_quality_ssa_unzipped_folder/'

default_experts_twitter_stream_settings['min_phrase_length'] = 1
default_experts_twitter_stream_settings['max_phrase_length'] = 1
default_experts_twitter_stream_settings['threshold_for_document_to_be_in_cluster'] = 0.5

previousTime = None
evaluation = Evaluation()

stream_cda_stats_file = time_to_process_points+'stats/stream_cda'
ssa_stats_file = time_to_process_points+'stats/ssa'
ssa_mr_stats_file = time_to_process_points+'stats/ssa_mr'

plot_info = { 
             'stream_cda': {'id': 'Stream-CDA', 'label': 'Stream-CDA', 'color': '#7109AA', 'marker': '*'},
             'ssa': {'id': 'ssa', 'label': 'Iterative CDA', 'color': '#FD0006', 'marker': 'o'},
             'ssa_mr': {'id': 'ssa_mr', 'label': 'MR CDA', 'color': '#5AF522', 'marker': '>'}
             }

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
    
def iterateUserDocuments(fileName):
    dataForAggregation = defaultdict(Vector)
    textToIdMap = defaultdict(int)
    for tweet in FileIO.iterateJsonFromFile(fileName):
        textVector = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage(tweet, **default_experts_twitter_stream_settings).vector
        textIdVector = Vector()
        for phrase in textVector: 
            if phrase not in textToIdMap: textToIdMap[phrase]=str(len(textToIdMap))
            textIdVector[textToIdMap[phrase]]=textVector[phrase]
        dataForAggregation[tweet['user']['screen_name'].lower()]+=textIdVector
    for k, v in dataForAggregation.iteritems(): yield k, v
    
def getStatsForSSA():
    batchSize = 10000
    default_experts_twitter_stream_settings['ssa_threshold']=0.75
    for id in range(21, 50):
        fileName = time_to_process_points+'%s/%s'%(batchSize,id)
        ts = time.time()
        sstObject = SimilarStreamAggregation(dict(iterateUserDocuments(fileName)), default_experts_twitter_stream_settings['ssa_threshold'])
        sstObject.estimate()
    #    documentClusters = list(sstObject.iterateClusters())
        iteration_data = {'iteration_time': time.time()-ts, 'type': 'ssa', 'number_of_messages': batchSize*(id+1), 'batch_size': batchSize}
        FileIO.writeToFileAsJson(iteration_data, ssa_stats_file)
        
def getStatsForSSAMR():
    batchSize = 50000
    default_experts_twitter_stream_settings['ssa_threshold']=0.75
    for id in range(0, 10):
        ts = time.time()
        fileName = time_to_process_points+'%s/%s'%(batchSize,id)
        iteration_file = '%s_%s'%(batchSize, id)
        print 'Generating data for ', iteration_file
        with open(iteration_file, 'w') as fp: [fp.write(CJSONProtocol.write('x', [doc1, doc2])+'\n') for doc1, doc2 in combinations(iterateUserDocuments(fileName),2)]
        os.system('hadoop fs -put %s %s'%(iteration_file, hdfsUnzippedPath))    
        StreamSimilarityAggregationMR.estimate(hdfsUnzippedPath+'/%s'%iteration_file, args='-r hadoop'.split(), 
                                        jobconf={'mapred.map.tasks':25, 'mapred.task.timeout': 7200000, 'mapred.reduce.tasks':25})
        
        os.system('hadoop fs -rmr %s'%(hdfsUnzippedPath+'/%s'%iteration_file))
        os.system('rm -rf %s'%iteration_file)
        iteration_data = {'iteration_time': time.time()-ts, 'type': 'ssa_mr', 'number_of_messages': batchSize*(id+1), 'batch_size': batchSize}
        FileIO.writeToFileAsJson(iteration_data, ssa_mr_stats_file)
        
def getIterator(id):
    for line in FileIO.iterateJsonFromFile(time_to_process_points+'stats/%s'%id): yield line

def plotMessagesProcessedWithTime(iterators):
    time_limit = 2300
    for iterator, info in iterators:
        dataX, dataY = [], []
        if not info['id'].startswith('ssa'):
            for data in iterator: 
                if data['iteration_time']<time_limit: dataX.append(data['iteration_time']), dataY.append(data['number_of_messages'])
        else:
            iteration_time = 0
            for data in iterator: 
                if data['batch_size']==10000:
                    iteration_time+=data['iteration_time']
                    if iteration_time<time_limit: dataX.append(iteration_time), dataY.append(data['number_of_messages'])
#        print info, dataX, dataY
        plt.plot(dataX, [y/10**3 for y in dataY], lw=2, label=info['label'], color=info['color'])
#    plt.xlim(xmin=15, xmax=3000)
    plt.legend(loc=2)
    plt.xlabel(getLatexForString('Time (s)')); plt.ylabel(getLatexForString('\# of messages (10^3)')); plt.title(getLatexForString('Message processing ability of the algorithms'))
    plt.plot()
    plt.savefig('messagesProcessedWithTime.pdf')
    plt.savefig('messagesProcessedWithTime.eps')

#generateData()
#getStatsForCDA()
#getStatsForSSA()
#getStatsForSSAMR()
plotMessagesProcessedWithTime([(getIterator('stream_cda'), plot_info['stream_cda']),
                               (getIterator('ssa'), plot_info['ssa']),
                               (getIterator('ssa_mr'), plot_info['ssa_mr'])])