'''
Created on Jun 30, 2011

@author: kykamath
'''
import sys, os, time
import matplotlib
#matplotlib.use("WXAgg")
sys.path.append('../')
from settings import experts_twitter_stream_settings, houston_twitter_stream_settings
os.environ["PATH"] = os.environ["PATH"]+os.pathsep+'/opt/local/bin'
from twitter_streams_clustering import TwitterCrowdsSpecificMethods,\
    TwitterIterators, getExperts
from hd_streams_clustering import HDStreaminClustering
from datetime import datetime, timedelta
from library.file_io import FileIO
from library.twitter import getStringRepresentationForTweetTimestamp, getDateTimeObjectFromTweetTimestamp
from library.classes import GeneralMethods, Settings, timeit
from library.plotting import getLatexForString, plotMethods
from library.clustering import EvaluationMetrics
from operator import itemgetter
from classes import Crowd, StreamCluster
import numpy as np
from pymongo import Connection
import networkx as nx
from matplotlib import pyplot as plt
from Queue import Queue
import matplotlib as mpl
from collections import defaultdict

mongodb_connection = Connection('sarge', 27017)
tweets = mongodb_connection.old_hou.Tweet
users = mongodb_connection.old_hou.User
houston_data_folder = '/mnt/chevron/kykamath/data/twitter/houston/'

class GenerateHoustonTweetsData:
    userIdToScreenNameMap = {}
    @staticmethod
    def getScreenName(uid):
        if uid not in GenerateHoustonTweetsData.userIdToScreenNameMap: 
            userObject = users.find_one({'_id': uid}, fields=['sn'])
            if userObject!=None: GenerateHoustonTweetsData.userIdToScreenNameMap[uid]=userObject['sn']
        return GenerateHoustonTweetsData.userIdToScreenNameMap.get(uid, None)
    @staticmethod
    def writeTweetsForDay(currentDay):
        fileName = houston_data_folder+FileIO.getFileByDay(currentDay)
        for tweet in tweets.find({'ca': {'$gt':currentDay, '$lt': currentDay+timedelta(seconds=86399)}}, fields=['ca', 'tx', 'uid']):
            screenName = GenerateHoustonTweetsData.getScreenName(tweet['uid'])
            if screenName!=None: 
                data = {'id': tweet['_id'], 'text': tweet['tx'], 'created_at':getStringRepresentationForTweetTimestamp(tweet['ca']), 'user':{'screen_name': GenerateHoustonTweetsData.getScreenName(tweet['uid'])}}
                FileIO.writeToFileAsJson(data, fileName) 
        os.system('gzip %s'%fileName)
    @staticmethod
    def generateHoustonData():
        currentDay = datetime(2011,1,23)
        endingDay = datetime(2011,5,31)
        while currentDay<=endingDay:
            print 'Generating data for: ', currentDay
            GenerateHoustonTweetsData.writeTweetsForDay(currentDay)
            currentDay+=timedelta(days=1)

class ClusterIterators():
    ''' Iterator for clusters. '''
    @staticmethod
    def iterateExpertClusters(startingDay=datetime(2011,3,19), endingDay=datetime(2011,3, 30)):
#    def iterateExpertClusters(startingDay=datetime(2011,3,19), endingDay=datetime(2011,4,7)):
        while startingDay<=endingDay:
            for line in FileIO.iterateJsonFromFile(experts_twitter_stream_settings.lsh_clusters_folder+FileIO.getFileByDay(startingDay)): 
                currentTime = getDateTimeObjectFromTweetTimestamp(line['time_stamp'])
                for clusterMap in line['clusters']: yield (currentTime, TwitterCrowdsSpecificMethods.getClusterFromMapFormat(clusterMap))
            startingDay+=timedelta(days=1)
    @staticmethod
    def iterateHoustonClusters(startingDay=datetime(2010,11,1), endingDay=datetime(2010,11,19)):
        while startingDay<=endingDay:
            for line in FileIO.iterateJsonFromFile(houston_twitter_stream_settings.lsh_clusters_folder+FileIO.getFileByDay(startingDay)): 
                currentTime = getDateTimeObjectFromTweetTimestamp(line['time_stamp'])
                for clusterMap in line['clusters']: yield (currentTime, TwitterCrowdsSpecificMethods.getClusterFromMapFormat(clusterMap))
            startingDay+=timedelta(days=1)
        
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
    @timeit
    def writeClusters(hdStreamClusteringObject, currentMessageTime):
        print '\n\n\nEntering:', currentMessageTime, len(hdStreamClusteringObject.phraseTextAndDimensionMap), len(hdStreamClusteringObject.phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
        iterationData = {'time_stamp': getStringRepresentationForTweetTimestamp(currentMessageTime),
                         'clusters': map(TwitterCrowdsSpecificMethods.getClusterInMapFormat, [cluster for cluster, _ in sorted(StreamCluster.iterateByAttribute(hdStreamClusteringObject.clusters.values(), 'length'), key=itemgetter(1), reverse=True)]),
                         'settings': Settings.getSerialzedObject(hdStreamClusteringObject.stream_settings)
                         }
        FileIO.writeToFileAsJson(iterationData, hdStreamClusteringObject.stream_settings['lsh_clusters_folder']+FileIO.getFileByDay(currentMessageTime))
        print 'Leaving: ', currentMessageTime, len(hdStreamClusteringObject.phraseTextAndDimensionMap), len(hdStreamClusteringObject.phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)

def generateClusters():
    TwitterStreamAnalysis(**experts_twitter_stream_settings).generateClusters(TwitterIterators.iterateTweetsFromExperts())
#    TwitterStreamAnalysis(**houston_twitter_stream_settings).generateClusters(TwitterIterators.iterateTweetsFromHouston())

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

class AnalyzeData:
    crowdMap, clusterIdToCrowdIdMap, crowdIdToClusterIdMap, clusterMap = {}, {}, {}, {}
    @staticmethod
    def constructCrowdDataStructures(dataIterator):
        for currentTime, cluster in dataIterator():
            crowdId, newCrowdAdded = None, False
            cluster.currentTime = currentTime
            AnalyzeData.clusterMap[cluster.clusterId]=cluster
            for clusterId in cluster.mergedClustersList: 
                if clusterId in AnalyzeData.clusterIdToCrowdIdMap: crowdId=AnalyzeData.clusterIdToCrowdIdMap[clusterId]; break
            if crowdId==None:
                crowdId=cluster.mergedClustersList[0]
                AnalyzeData.crowdMap[crowdId]=Crowd(cluster, currentTime)
#                cluster.mergedClustersList=cluster.mergedClustersList[1:]
                newCrowdAdded = True
            else: AnalyzeData.crowdMap[crowdId].append(cluster, currentTime)
            if crowdId==None: raise Exception('Crowd id cannot be None.')
            AnalyzeData.clusterIdToCrowdIdMap[cluster.clusterId]=crowdId
            if not newCrowdAdded: mergedClustersList = cluster.mergedClustersList[:]
            else: mergedClustersList = cluster.mergedClustersList[1:][:]
            for clusterId in mergedClustersList: 
                if clusterId in AnalyzeData.clusterIdToCrowdIdMap:  AnalyzeData.crowdMap[AnalyzeData.clusterIdToCrowdIdMap[clusterId]].updateOutGoingCrowd(crowdId), AnalyzeData.crowdMap[crowdId].updateInComingCrowd(AnalyzeData.clusterIdToCrowdIdMap[clusterId])
        AnalyzeData.constructCrowdIdToClusterIdMap()
    @staticmethod
    def constructCrowdIdToClusterIdMap():
        for k, v in AnalyzeData.clusterIdToCrowdIdMap.iteritems(): 
            if v not in AnalyzeData.crowdIdToClusterIdMap: AnalyzeData.crowdIdToClusterIdMap[v]=[]
            AnalyzeData.crowdIdToClusterIdMap[v].append(k)
    @staticmethod
    def getCrowdHierarchy(clusterId): 
#        AnalyzeData.constructCrowdIdToClusterIdMap()
        hierarchy, crowdIdQueue = {}, Queue()
        def getMainBranch(clusterId): 
            def getClusterInt(id): return int(id.split('_')[1])
            sortedMainBranchList = sorted([AnalyzeData.clusterIdToCrowdIdMap[clusterId]]+AnalyzeData.crowdIdToClusterIdMap[AnalyzeData.clusterIdToCrowdIdMap[clusterId]], key=getClusterInt)
            return (sortedMainBranchList[0], dict([(sortedMainBranchList[i], sortedMainBranchList[i+1]) for i in range(len(sortedMainBranchList)-1)]))
        def addClustersNotInHierachyToQueue(crowdId):
                clusters = AnalyzeData.crowdMap[crowdId].clusters
                for id, mergedClustersList in [(cluster.clusterId, cluster.mergedClustersList) for cluster in clusters.itervalues()]:
                    for clusterIdNotInHierarchy in filter(lambda x: x not in hierarchy, mergedClustersList): crowdIdQueue.put((id, clusterIdNotInHierarchy))
        crowdIdQueue.put((None, clusterId))
        while not crowdIdQueue.empty():
            childId, clusterId = crowdIdQueue.get()
            if clusterId in AnalyzeData.clusterIdToCrowdIdMap:
                crowdId, mainBranch = getMainBranch(clusterId)
                if childId: mainBranch[clusterId]=childId
                hierarchy.update(mainBranch)
                addClustersNotInHierachyToQueue(crowdId)
                if AnalyzeData.crowdMap[crowdId].outGoingCrowd!=None: addClustersNotInHierachyToQueue(AnalyzeData.crowdMap[crowdId].outGoingCrowd)
        return hierarchy
    @staticmethod
    def reset(): AnalyzeData.crowdMap, AnalyzeData.clusterIdToCrowdIdMap, AnalyzeData.crowdIdToClusterIdMap = {}, {}, {}

class Plot:
    def __init__(self, **stream_settings):
        self.stream_settings = stream_settings
    def lifeSpanDistribution(self, returnAxisValuesOnly=True):
        AnalyzeData.reset(), AnalyzeData.constructCrowdDataStructures(self.stream_settings['data_iterator'])
        y,x= np.histogram([AnalyzeData.crowdMap[crowd].lifespan for crowd in AnalyzeData.crowdMap], bins=15)
        plt.semilogy(x[:-1], y, color=self.stream_settings['plot_color'], lw=2, label=self.stream_settings['plot_label'])
        plt.xlabel(getLatexForString('Lifespan'))
        plt.ylabel(getLatexForString('\# of crowds'))
        plt.title(getLatexForString('Crowd lifespan distribution'))
        plt.legend()
        if returnAxisValuesOnly: plt.show()
    def crowdSizeDistribution(self, returnAxisValuesOnly=True):
        AnalyzeData.reset(), AnalyzeData.constructCrowdDataStructures(self.stream_settings['data_iterator'])
        y,x= np.histogram([AnalyzeData.crowdMap[crowd].crowdSize for crowd in AnalyzeData.crowdMap], bins=15)
        plt.semilogy(x[:-1], y, color=self.stream_settings['plot_color'], lw=2, label=self.stream_settings['plot_label'])
        plt.xlabel(getLatexForString('Crowd Size'))
        plt.ylabel(getLatexForString('\# of crowds'))
        plt.title(getLatexForString('Crowd size distribution'))
        plt.legend()
        if returnAxisValuesOnly: plt.show()
    def crowdSizeToLifeSpanPlot(self, returnAxisValuesOnly=True):
        AnalyzeData.reset(), AnalyzeData.constructCrowdDataStructures(self.stream_settings['data_iterator'])
        crowdSizeX, lifeSpanY = [], []
        for crowd in AnalyzeData.crowdMap: crowdSizeX.append(AnalyzeData.crowdMap[crowd].crowdSize), lifeSpanY.append(AnalyzeData.crowdMap[crowd].lifespan)
        plt.loglog(crowdSizeX, lifeSpanY, 'o', color=self.stream_settings['plot_color'], label=self.stream_settings['plot_label'])
        plt.xlabel(getLatexForString('Crowd Size'))
        plt.ylabel(getLatexForString('Lifespan'))
        plt.title(getLatexForString('Crowd size Vs Lifespan'))
        plt.legend()
        if returnAxisValuesOnly: plt.show()
    def sampleCrowds(self):
        # Set dates for experts as startingDay=datetime(2011,3,19), endingDay=datetime(2011,3, 30) with a minimum of 7 users at a time.
        AnalyzeData.reset(), AnalyzeData.constructCrowdDataStructures(self.stream_settings['data_iterator'])
        fig = plt.figure(); ax = fig.gca()
#        expectedTags = set(['#redsox', '#mlb', '#sfgiants', '#49ers', '#mariners', '#twins', '#springtraining', '#mets', '#reds'])
#        expectedTags = set(['#ctia']); title = 'CTIA 2011'
#        expectedTags = set(['#55', '#hcr', '#hcrbday', '#oklahomas', '#aca', '#hcworks', '#npr', '#teaparty'])
#        expectedTags = set(['#budget11', '#taxdodgers', '#budget', '#pmqs', '#budget11', '#indybudget'])
#        expectedTags = set(['#egypt2dc', '#libyan', '#yemen', '#egypt', '#syria', '#gaddaficrimes', '#damascus', '#jan25', 
#                '#daraa', '#feb17', '#gaddafi', '#libya', '#feb17', '#gadhafi', '#muslimbrotherhood', '#gaddafis']); title = 'Middle East'
        expectedTags = set(['#libya']); title = 'Libya'
        for crowd in self._filteredCrowdIterator():
            if expectedTags.intersection(set(list(crowd.hashtagDimensions))):
                x, y = zip(*[(datetime.fromtimestamp(clusterGenerationTime), len(crowd.clusters[clusterGenerationTime].documentsInCluster)) for clusterGenerationTime in sorted(crowd.clusters)])
                plt.plot_date(x, y, '-', color=GeneralMethods.getRandomColor(), lw=2, label=' '.join([crowd.crowdId]+list(crowd.hashtagDimensions)[:1]))
        fig.autofmt_xdate(rotation=30)
        ax.xaxis.set_major_locator(matplotlib.dates.HourLocator(interval=24))
        ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%a %d %b'))
#        plt.legend()
        plt.xlim((datetime(2011, 3, 19), datetime(2011, 3, 30)))
        plt.title(getLatexForString('Crowds for '+title))
        plt.ylabel(getLatexForString('Crowd size'))
        plt.show()
    def crowdHierachy(self):
        AnalyzeData.reset(), AnalyzeData.constructCrowdDataStructures(self.stream_settings['data_iterator'])
        for crowd in self._filteredCrowdIterator():
            clusterId = crowd.clusters[sorted(crowd.clusters.keys())[0]].clusterId
            print clusterId
            print ' '.join(list(crowd.hashtagDimensions))
            self._plotHierarchy(AnalyzeData.getCrowdHierarchy(clusterId))
    def sampleCrowdHierarchy(self):
        AnalyzeData.reset(), AnalyzeData.constructCrowdDataStructures(self.stream_settings['data_iterator'])
        clusterId = 'cluster_51958'
        self._plotHierarchy(AnalyzeData.getCrowdHierarchy(clusterId))
    def sampleCrowdUsers(self):
        AnalyzeData.reset(), AnalyzeData.constructCrowdDataStructures(self.stream_settings['data_iterator'])
        clusterId = 'cluster_51958'
        crowd = AnalyzeData.crowdMap[AnalyzeData.clusterIdToCrowdIdMap[clusterId]]
        j=0
        for k, v in sorted(crowd.clusters.iteritems(), key=itemgetter(0)):
            if j%4==0: print datetime.fromtimestamp(k).strftime('%H:%M'), ' & ' , ',\\ '.join(sorted([i.lower().replace('_', '\_') for i in v.documentsInCluster])), '\\\\'
            j+=1
        print datetime.fromtimestamp(k).strftime('%H:%M'), ' & ' , ',\\ '.join(sorted([i.lower().replace('_', '\_') for i in v.documentsInCluster])), '\\\\'
        print 'A sample crowd discussing Samsung Android mobile phones during CTIA on', datetime.fromtimestamp(k).strftime('%d %B, %Y')
    def _filteredCrowdIterator(self):
        for crowd in [crowd for crowd in AnalyzeData.crowdMap.itervalues()
                                if crowd.lifespan>10 and 
                                   crowd.lifespan<50 and
#                                   crowd.startTime>datetime(2011,3,19) and 
#                                   crowd.endTime<datetime(2011,3,23) and
                                   crowd.hashtagDimensions and 
                                   crowd.maxClusterSize>7]: yield crowd
    def _getCrowdsInAHierarchy(self, hierarchy): return set([AnalyzeData.clusterIdToCrowdIdMap[clusterId] for clusterId in hierarchy.keys()+hierarchy.values() if clusterId in AnalyzeData.clusterIdToCrowdIdMap])
    def _plotHierarchy(self, hierarchy):
        graph, labels = nx.DiGraph(), {}
        F=plt.gcf()
        for u,v in hierarchy.iteritems(): 
            if u in AnalyzeData.crowdMap: 
                labels[u]=' '.join(list(AnalyzeData.crowdMap[u].hashtagDimensions))
                labels[v]= AnalyzeData.clusterMap[v].currentTime.strftime('%H:%M')
#            elif u in AnalyzeData.clusterMap: labels[u]= AnalyzeData.clusterMap[u].currentTime.strftime('%H:%M')# + ' ' + ', '.join(sorted([i.lower() for i in AnalyzeData.clusterMap[u].documentsInCluster]))
#            if v in AnalyzeData.clusterMap: labels[v]= AnalyzeData.clusterMap[v].currentTime.strftime('%H:%M') #+' ' + ', '.join(sorted([i.lower() for i in AnalyzeData.clusterMap[v].documentsInCluster]))
            graph.add_edge(u, v)
        for n in graph.nodes_iter():
            if graph.in_degree(n)==2 or graph.out_degree(n)==0: labels[n]= AnalyzeData.clusterMap[n].currentTime.strftime('%H:%M')
        F.set_size_inches( (4.86,3.0) )
        pos=nx.graphviz_layout(graph, prog='dot',args='')
        nx.draw(graph, pos, alpha=1.0, node_size=10, with_labels=True, labels=labels, font_size=8, arrows=True, node_color='#5AF522')
        plt.show()
    @staticmethod
    def getLifeSpanDistributionPlot(): 
        plotMethods([Plot(**experts_twitter_stream_settings).lifeSpanDistribution, Plot(**houston_twitter_stream_settings).lifeSpanDistribution])
    @staticmethod
    def getCrowdSizeDistributionPlot(): 
        plotMethods([Plot(**experts_twitter_stream_settings).crowdSizeDistribution, Plot(**houston_twitter_stream_settings).crowdSizeDistribution])
    @staticmethod
    def getCrowdSizeToLifeSpanPlot(): 
        plotMethods([Plot(**experts_twitter_stream_settings).crowdSizeToLifeSpanPlot, Plot(**houston_twitter_stream_settings).crowdSizeToLifeSpanPlot])

def getStreamStats(streamTweetsIterator):
    '''
        Experts stats:
        # of users:  4804
        # of tweets:  1614510
        # of tweets per tu (mean, var):  186.497631974 7860.12570191
    '''
    numberOfTweets, users, distributionPerTU = 0, set(), defaultdict(int)
    for tweet in streamTweetsIterator: 
        users.add(tweet['user']['screen_name'])
        distributionPerTU[GeneralMethods.getEpochFromDateTimeObject(getDateTimeObjectFromTweetTimestamp(tweet['created_at']))//300]+=1
        numberOfTweets+=1
    print '# of users: ', len(users)
    print '# of tweets: ', numberOfTweets 
    print '# of tweets per tu (mean, var): ', np.mean(distributionPerTU.values()), np.var(distributionPerTU.values())

if __name__ == '__main__':
#    GenerateHoustonTweetsData.generateHoustonData()
#    generateClusters()

#    experts_twitter_stream_settings['data_iterator'] = ClusterIterators.iterateExpertClusters
#    houston_twitter_stream_settings['data_iterator'] = ClusterIterators.iterateHoustonClusters
    
#    Plot.getLifeSpanDistributionPlot()
#    Plot.getCrowdSizeDistributionPlot()
#    Plot.getCrowdSizeToLifeSpanPlot()
#    Plot(**experts_twitter_stream_settings).sampleCrowds()
#    Plot(**experts_twitter_stream_settings).crowdHierachy()
#    Plot(**experts_twitter_stream_settings).sampleCrowdUsers()
#    Plot(**experts_twitter_stream_settings).sampleCrowdHierarchy()
    
#    print 'Experts stats'
#    getStreamStats(TwitterIterators.iterateTweetsFromExperts(expertsDataStartTime=datetime(2011,3,19), expertsDataEndTime=datetime(2011,4,20)))
    print 'Houston stats'
    getStreamStats(TwitterIterators.iterateTweetsFromHouston(houstonDataStartTime=datetime(2010,11,1), houstonDataEndTime=datetime(2010,12,2)))
