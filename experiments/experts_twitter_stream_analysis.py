'''
Created on Jun 30, 2011

@author: kykamath
'''
import sys, os
sys.path.append('../')
from settings import experts_twitter_stream_settings
os.environ["PATH"] = os.environ["PATH"]+os.pathsep+'/opt/local/bin'
from twitter_streams_clustering import TwitterCrowdsSpecificMethods,\
    TwitterIterators, getExperts
from hd_streams_clustering import HDStreaminClustering
from datetime import datetime, timedelta
from library.file_io import FileIO
from library.twitter import getStringRepresentationForTweetTimestamp, getDateTimeObjectFromTweetTimestamp
from library.classes import GeneralMethods, Settings
from library.plotting import getLatexForString
from library.clustering import EvaluationMetrics
from operator import itemgetter
from classes import Crowd, StreamCluster
import numpy as np
import networkx as nx
from matplotlib import pyplot as plt
from Queue import Queue

#startingDay=datetime(2011,3,19)
#endingDay=datetime(2011,3,22)

#def iterateExpertClusters(startingDay=startingDay, endingDay=endingDay):
#    while startingDay<=endingDay:
#        for line in FileIO.iterateJsonFromFile(experts_twitter_stream_settings.lsh_clusters_folder+FileIO.getFileByDay(startingDay)): 
#            currentTime = getDateTimeObjectFromTweetTimestamp(line['time_stamp'])
#            for clusterMap in line['clusters']: yield (currentTime, TwitterCrowdsSpecificMethods.getClusterFromMapFormat(clusterMap))
#        startingDay+=timedelta(days=1)
        
class TwitterStreamAnalysis:
    def __init__(self, **stream_settings):
        self.stream_settings = stream_settings
        self.stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
        self.stream_settings['combine_clusters_method'] = TwitterCrowdsSpecificMethods.combineClusters
#        self.stream_settings['cluster_filtering_method'] = TwitterCrowdsSpecificMethods.emptyClusterFilteringMethod
    def generateClusters(self, iterator):
        self.stream_settings['cluster_analysis_method'] = TwitterStreamAnalysis.writeClusters
        HDStreaminClustering(**self.stream_settings).cluster(iterator)
    @staticmethod
    def writeClusters(hdStreamClusteringObject, currentMessageTime):
        print '\n\n\nEntering:', currentMessageTime, len(hdStreamClusteringObject.phraseTextAndDimensionMap), len(hdStreamClusteringObject.phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
        iterationData = {'time_stamp': getStringRepresentationForTweetTimestamp(currentMessageTime),
                         'clusters': map(TwitterCrowdsSpecificMethods.getClusterInMapFormat, [cluster for cluster, _ in sorted(StreamCluster.iterateByAttribute(hdStreamClusteringObject.clusters.values(), 'length'), key=itemgetter(1), reverse=True)]),
                         'settings': Settings.getSerialzedObject(hdStreamClusteringObject.stream_settings)
                         }
        FileIO.writeToFileAsJson(iterationData, hdStreamClusteringObject.stream_settings.lsh_clusters_folder+FileIO.getFileByDay(currentMessageTime))
        print 'Leaving: ', currentMessageTime, len(hdStreamClusteringObject.phraseTextAndDimensionMap), len(hdStreamClusteringObject.phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)

def generateClusters():
    TwitterStreamAnalysis(**experts_twitter_stream_settings).generateClusters(TwitterIterators.iterateTweetsFromExperts())

#class GenerateData:
#    @staticmethod
#    def expertClusters():
#        def analyzeIterationData(hdStreamClusteringObject, currentMessageTime):
#            print '\n\n\nEntering:', currentMessageTime, len(hdStreamClusteringObject.phraseTextAndDimensionMap), len(hdStreamClusteringObject.phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
#            iterationData = {'time_stamp': getStringRepresentationForTweetTimestamp(currentMessageTime),
#                             'clusters': map(TwitterCrowdsSpecificMethods.getClusterInMapFormat, [cluster for cluster, _ in sorted(StreamCluster.iterateByAttribute(hdStreamClusteringObject.clusters.values(), 'length'), key=itemgetter(1), reverse=True)]),
#                             'settings': experts_twitter_stream_settings.convertToSerializableObject()
#                             }
#            FileIO.writeToFileAsJson(iterationData, experts_twitter_stream_settings.lsh_clusters_folder+FileIO.getFileByDay(currentMessageTime))
#            print 'Leaving: ', currentMessageTime, len(hdStreamClusteringObject.phraseTextAndDimensionMap), len(hdStreamClusteringObject.phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
#        
#        experts_twitter_stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
##        experts_twitter_stream_settings['cluster_filtering_method']=TwitterCrowdsSpecificMethods.clusterFilteringMethod
#        experts_twitter_stream_settings['combine_clusters_method'] = TwitterCrowdsSpecificMethods.combineClusters
#        experts_twitter_stream_settings['cluster_analysis_method'] = TwitterCrowdsSpecificMethods.clusterAnalysisMethod
#        HDStreaminClustering(**experts_twitter_stream_settings).cluster(TwitterIterators.iterateTweetsFromExperts())

#class AnalyzeData:
#    crowdMap, clusterIdToCrowdIdMap, crowdIdToClusterIdMap = {}, {}, {}
#    @staticmethod
#    def constructCrowdDataStructures(dataIterator):
#        for currentTime, cluster in dataIterator():
#            crowdId, newCrowdAdded = None, False
#            for clusterId in cluster.mergedClustersList: 
#                if clusterId in AnalyzeData.clusterIdToCrowdIdMap: crowdId=AnalyzeData.clusterIdToCrowdIdMap[clusterId]; break
#            if crowdId==None:
#                crowdId=cluster.mergedClustersList[0]
#                AnalyzeData.crowdMap[crowdId]=Crowd(cluster, currentTime)
##                cluster.mergedClustersList=cluster.mergedClustersList[1:]
#                newCrowdAdded = True
#            else: AnalyzeData.crowdMap[crowdId].append(cluster, currentTime)
#            if crowdId==None: raise Exception('Crowd id cannot be None.')
#            AnalyzeData.clusterIdToCrowdIdMap[cluster.clusterId]=crowdId
#            if not newCrowdAdded: mergedClustersList = cluster.mergedClustersList[:]
#            else: mergedClustersList = cluster.mergedClustersList[1:][:]
#            for clusterId in mergedClustersList: 
#                if clusterId in AnalyzeData.clusterIdToCrowdIdMap:  AnalyzeData.crowdMap[AnalyzeData.clusterIdToCrowdIdMap[clusterId]].updateOutGoingCrowd(crowdId), AnalyzeData.crowdMap[crowdId].updateInComingCrowd(AnalyzeData.clusterIdToCrowdIdMap[clusterId])
#        AnalyzeData.constructCrowdIdToClusterIdMap()
#    @staticmethod
#    def constructCrowdIdToClusterIdMap():
#        for k, v in AnalyzeData.clusterIdToCrowdIdMap.iteritems(): 
#            if v not in AnalyzeData.crowdIdToClusterIdMap: AnalyzeData.crowdIdToClusterIdMap[v]=[]
#            AnalyzeData.crowdIdToClusterIdMap[v].append(k)
#    @staticmethod
#    def getCrowdsPurity():
#        expertsToClassMap = dict([(k, v['class']) for k,v in getExperts(byScreenName=True).iteritems()])
#        print np.mean(map(lambda crowd: crowd.getCrowdQuality(EvaluationMetrics.purity, expertsToClassMap), AnalyzeData.crowdMap.itervalues()))
#    @staticmethod
#    def getCrowdHierarchy(clusterId): 
##        AnalyzeData.constructCrowdIdToClusterIdMap()
#        hierarchy, crowdIdQueue = {}, Queue()
#        def getMainBranch(clusterId): 
#            def getClusterInt(id): return int(id.split('_')[1])
#            sortedMainBranchList = sorted([AnalyzeData.clusterIdToCrowdIdMap[clusterId]]+AnalyzeData.crowdIdToClusterIdMap[AnalyzeData.clusterIdToCrowdIdMap[clusterId]], key=getClusterInt)
#            return (sortedMainBranchList[0], dict([(sortedMainBranchList[i], sortedMainBranchList[i+1]) for i in range(len(sortedMainBranchList)-1)]))
#        def addClustersNotInHierachyToQueue(crowdId):
#                clusters = AnalyzeData.crowdMap[crowdId].clusters
#                for id, mergedClustersList in [(cluster.clusterId, cluster.mergedClustersList) for cluster in clusters.itervalues()]:
#                    for clusterIdNotInHierarchy in filter(lambda x: x not in hierarchy, mergedClustersList): crowdIdQueue.put((id, clusterIdNotInHierarchy))
#        crowdIdQueue.put((None, clusterId))
#        while not crowdIdQueue.empty():
#            childId, clusterId = crowdIdQueue.get()
#            if clusterId in AnalyzeData.clusterIdToCrowdIdMap:
#                crowdId, mainBranch = getMainBranch(clusterId)
#                if childId: mainBranch[clusterId]=childId
#                hierarchy.update(mainBranch)
#                addClustersNotInHierachyToQueue(crowdId)
#                if AnalyzeData.crowdMap[crowdId].outGoingCrowd!=None: addClustersNotInHierachyToQueue(AnalyzeData.crowdMap[crowdId].outGoingCrowd)
#        return hierarchy
#        
#class Plot:
#    @staticmethod
#    def lifeSpanDistribution():
#        y,x= np.histogram([AnalyzeData.crowdMap[crowd].lifespan for crowd in AnalyzeData.crowdMap], bins=15)
#        plt.semilogy(x[:-1], y, color='#F7AA45', lw=2)
#        plt.xlabel(getLatexForString('Lifespan'))
#        plt.ylabel(getLatexForString('\# of crowds'))
#        plt.title(getLatexForString('Crowd lifespan distribution'))
#        plt.show()
#    @staticmethod
#    def sampleCrowds():
#        filteredCrowds = [crowd for crowd in AnalyzeData.crowdMap.itervalues()
#                            if crowd.lifespan>10 and crowd.lifespan<50 and
#                              crowd.startTime>datetime(2011,3,19) and crowd.endTime<datetime(2011,3,22) and
#                                crowd.hashtagDimensions][5:10]
#        for crowd in filteredCrowds:
#            x, y = zip(*[(clusterGenerationTime, len(crowd.clusters[clusterGenerationTime].documentsInCluster)) for clusterGenerationTime in sorted(crowd.clusters)])
#            if max(y)<30 and min(y)<5:
#                print crowd.crowdId, crowd.ends, crowd.outGoingCrowd, crowd.startTime, crowd.crowdId, crowd.lifespan, GeneralMethods.getRandomColor(), x, y, list(crowd.hashtagDimensions)[:3]
#                plt.plot(x, y, color=GeneralMethods.getRandomColor(), lw=2, label=' '.join([crowd.crowdId]+list(crowd.hashtagDimensions)[:1]))
#        plt.legend()
#        plt.show()
#    @staticmethod
#    def crowdHierachy():
#        observedClusters = set()
#        for clusterId in AnalyzeData.clusterIdToCrowdIdMap:
##        for clusterId in ['cluster_9235','cluster_38873','cluster_63568','cluster_76865']:
#            if clusterId not in observedClusters:
#                hierarchy = AnalyzeData.getCrowdHierarchy(clusterId)
#                observedClusters=observedClusters.union(set(hierarchy.iterkeys()))
#                graph, labels = nx.DiGraph(), {}
#                for u,v in hierarchy.iteritems(): 
#                    if u in AnalyzeData.crowdMap: labels[u]=' '.join(list(AnalyzeData.crowdMap[u].hashtagDimensions))
#                    else: labels[u]=''
#                    labels[v]=''
#                    graph.add_edge(u, v)
#                pos=nx.graphviz_layout(graph, prog='dot',args='')
#                nx.draw(graph, pos, alpha=0.3, node_size=1, with_labels=True, labels=labels, font_size=8, arrows=True, node_color='r')
#                plt.show()
        
if __name__ == '__main__':
    generateClusters()
#    GenerateData.expertClusters()
#    AnalyzeData.constructCrowdDataStructures(iterateExpertClusters)
#    AnalyzeData.getCrowdsPurity()

#    Plot.lifeSpanDistribution()
#    Plot.sampleCrowds()
#    Plot.crowdHierachy()
