'''
Created on Jun 30, 2011

@author: kykamath
'''
import sys
from library.classes import PlottingMethods, GeneralMethods
from library.clustering import EvaluationMetrics
sys.path.append('../')
from settings import experts_twitter_stream_settings
from twitter_streams_clustering import TwitterCrowdsSpecificMethods,\
    TwitterIterators, getExperts
from hd_streams_clustering import HDStreaminClustering
from datetime import datetime, timedelta
from streaming_lsh.classes import Cluster
from library.file_io import FileIO
from library.twitter import getStringRepresentationForTweetTimestamp, getDateTimeObjectFromTweetTimestamp
from operator import itemgetter
from classes import Crowd
import numpy as np
from matplotlib import pyplot as plt

startingDay=datetime(2011,3,19)
endingDay=datetime(2011,3,22)

def iterateExpertClusters(startingDay=startingDay, endingDay=endingDay):
    while startingDay<=endingDay:
        for line in FileIO.iterateJsonFromFile(experts_twitter_stream_settings.lsh_crowds_folder+FileIO.getFileByDay(startingDay)): 
            currentTime = getDateTimeObjectFromTweetTimestamp(line['time_stamp'])
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
    crowdMap = {}
    @staticmethod
    def getCrowdsPurity():
        expertsToClassMap = dict([(k, v['class']) for k,v in getExperts(byScreenName=True).iteritems()])
        print np.mean(map(lambda crowd: crowd.getCrowdQuality(EvaluationMetrics.purity, expertsToClassMap), AnalyzeData.crowdMap.itervalues()))
    @staticmethod
    def loadCrowds():
        clusterToCrowdMap = {}
        for currentTime, cluster in iterateExpertClusters():
            crowdId=None
            for clusterId in cluster.mergedClustersList: 
                if clusterId in clusterToCrowdMap: crowdId=clusterToCrowdMap[clusterId]; break
            if crowdId==None:
                crowdId=cluster.mergedClustersList[0]
                AnalyzeData.crowdMap[crowdId]=Crowd(cluster, currentTime)
                cluster.mergedClustersList=cluster.mergedClustersList[1:]
            else: AnalyzeData.crowdMap[crowdId].append(cluster, currentTime)
            if crowdId==None: raise Exception('Crowd id cannot be None.')
            clusterToCrowdMap[cluster.clusterId]=crowdId
            for clusterId in cluster.mergedClustersList: 
                if clusterId in clusterToCrowdMap:  AnalyzeData.crowdMap[clusterToCrowdMap[clusterId]].updatedMergesInto(crowdId)
    @staticmethod
    def plotLifeSpanDistribution():
        y,x= np.histogram([AnalyzeData.crowdMap[crowd].lifespan for crowd in AnalyzeData.crowdMap], bins=15)
        plt.semilogy(x[:-1], y, color='#F7AA45', lw=2)
        plt.xlabel(PlottingMethods.getLatexForString('Lifespan'))
        plt.ylabel(PlottingMethods.getLatexForString('\# of crowds'))
        plt.title(PlottingMethods.getLatexForString('Crowd lifespan distribution'))
        plt.show()
    @staticmethod
    def plotSampleCrowds():
#        filteredCrowds = [crowd for crowd in AnalyzeData.crowdMap.itervalues() if crowd.lifespan>1and crowd.lifespan<50 and crowd.hashtagDimensions][:10]
        filteredCrowds = [crowd for crowd in AnalyzeData.crowdMap.itervalues()
                            if crowd.lifespan>10 and crowd.lifespan<50 and
                              crowd.startTime>datetime(2011,3,19) and crowd.endTime<datetime(2011,3,22) and
                                crowd.hashtagDimensions][:10]
        for crowd in filteredCrowds:
            x, y = zip(*[(clusterGenerationTime, len(crowd.clusters[clusterGenerationTime].documentsInCluster)) for clusterGenerationTime in sorted(crowd.clusters)])
            if max(y)<30 and min(y)<5:
                print crowd.crowdId, crowd.ends, crowd.mergesInto, crowd.startTime, crowd.crowdId, crowd.lifespan, GeneralMethods.getRandomColor(), x, y, list(crowd.hashtagDimensions)[:3]
                plt.plot(x, y, color=GeneralMethods.getRandomColor(), lw=2, label=' '.join(list(crowd.hashtagDimensions)[:1]))
        plt.legend()
        plt.show()
        
if __name__ == '__main__':
#    GenerateData.expertClusters()


    AnalyzeData.loadCrowds()
#    AnalyzeData.getCrowdsPurity()
#    AnalyzeData.plotLifeSpanDistribution()
#    AnalyzeData.plotSampleCrowds()

#    for crowdId in ['cluster_8532', 'cluster_7287']:
#        crowd = AnalyzeData.crowdMap[crowdId]
#        print crowd.crowdId, crowd.ends, crowd.mergesInto

    for crowd in AnalyzeData.crowdMap:
        print crowd

