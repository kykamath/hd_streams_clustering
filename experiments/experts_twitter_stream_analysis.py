'''
Created on Jun 30, 2011

@author: kykamath
'''
import sys
from library.classes import GeneralMethods
from classes import Crowd
sys.path.append('../')
from settings import experts_twitter_stream_settings
from twitter_streams_clustering import TwitterCrowdsSpecificMethods,\
    TwitterIterators
from hd_streams_clustering import HDStreaminClustering
from datetime import datetime, timedelta
from streaming_lsh.classes import Cluster
from library.file_io import FileIO
from library.twitter import getStringRepresentationForTweetTimestamp, getDateTimeObjectFromTweetTimestamp
from operator import itemgetter

def iterateExpertClusters(startingDay=datetime(2011,3,19), endingDay=datetime(2011,3,20)):
    while startingDay<=endingDay:
        for line in FileIO.iterateJsonFromFile(experts_twitter_stream_settings.lsh_crowds_folder+FileIO.getFileByDay(startingDay)): 
            currentTime = GeneralMethods.getEpochFromDateTimeObject(getDateTimeObjectFromTweetTimestamp(line['time_stamp']))
            for clusterMap in line['clusters']: yield (currentTime, TwitterCrowdsSpecificMethods.getClusterFromMapFormat(clusterMap))
        startingDay+=timedelta(days=1)

class GenerateData:
    @staticmethod
    def expertClusters():
        def analyzeIterationData(hdStreamClusteringObject, currentMessageTime):
            print '\n\n\nEntering:', currentMessageTime, len(hdStreamClusteringObject.phraseTextAndDimensionMap), len(hdStreamClusteringObject.phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
            iterationData = {'time_stamp': getStringRepresentationForTweetTimestamp(currentMessageTime),
                             'clusters': map(TwitterCrowdsSpecificMethods.getClusterInMapFormat, [cluster for cluster, _ in sorted(Cluster.iterateByAttribute(hdStreamClusteringObject.clusters.values(), 'length'), key=itemgetter(1), reverse=True)]),
                             'settings': experts_twitter_stream_settings.convertToSerializableObject()
                             }
            FileIO.writeToFileAsJson(iterationData, experts_twitter_stream_settings.lsh_crowds_folder+FileIO.getFileByDay(currentMessageTime))
            print 'Leaving: ', currentMessageTime, len(hdStreamClusteringObject.phraseTextAndDimensionMap), len(hdStreamClusteringObject.phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
        
        experts_twitter_stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
        experts_twitter_stream_settings['combine_clusters_method'] = TwitterCrowdsSpecificMethods.combineClusters
        experts_twitter_stream_settings['analyze_iteration_data_method'] = analyzeIterationData
        hdsClustering = HDStreaminClustering(**experts_twitter_stream_settings)
        hdsClustering.cluster(TwitterIterators.iterateTweetsFromExperts())

class AnalyzeData:
    @staticmethod
    def crowdSamples():
        crowdMap, clusterToCrowdMap = {}, {}
        for currentTime, cluster in iterateExpertClusters():
            crowdId=None
            for clusterId in cluster.mergedClustersList: 
                if clusterId in clusterToCrowdMap: crowdId=clusterToCrowdMap[clusterId]; break
            if crowdId==None:
                crowdId=cluster.mergedClustersList[0]
                crowdMap[crowdId]=Crowd(cluster, currentTime)
                cluster.mergedClustersList=cluster.mergedClustersList[1:]
            else: crowdMap[crowdId].append(cluster, currentTime)
            if crowdId==None: raise Exception('Crowd id cannot be None.')
            clusterToCrowdMap[cluster.clusterId]=crowdId
            for clusterId in cluster.mergedClustersList: 
                if clusterId in clusterToCrowdMap:  crowdMap[clusterToCrowdMap[clusterId]].updatedMergesInto(crowdId)
        
if __name__ == '__main__':
#    GenerateData.expertClusters()

    AnalyzeData.crowdSamples()