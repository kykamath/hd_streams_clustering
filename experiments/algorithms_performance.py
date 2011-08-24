'''
Created on Aug 19, 2011

@author: kykamath
'''
import sys, time
from library.plotting import getLatexForString
from library.twitter import getStringRepresentationForTweetTimestamp,\
    getDateTimeObjectFromTweetTimestamp
sys.path.append('../')
from settings import experts_twitter_stream_settings
from hd_streams_clustering import HDStreaminClustering
from twitter_streams_clustering import TwitterIterators,\
    TwitterCrowdsSpecificMethods, getExperts
from library.math_modified import getLargestPrimeLesserThan
from library.clustering import EvaluationMetrics
from library.file_io import FileIO
from datetime import datetime
from collections import defaultdict
import numpy as np
from itertools import groupby
from operator import itemgetter
import matplotlib.pyplot as plt

def emptyUpdateDimensionsMethod(hdStreamClusteringObject, currentMessageTime): pass # print 'Comes to empty update dimensions'
def emptyClusterAnalysisMethod(hdStreamClusteringObject, currentMessageTime): pass # print 'Comes to empty analysis'
def emptyClusterFilteringMethod(hdStreamClusteringObject, currentMessageTime): pass # print 'Comes to empty filtering'

#previousSet = set()
#def modifiedUpdateDimensionsMethod(hdStreamClusteringObject, currentMessageTime): 
#    global previousSet
#    print len(hdStreamClusteringObject.phraseTextAndDimensionMap)
#    print 'Intersection', len(previousSet.intersection(set([l for l in hdStreamClusteringObject.phraseTextAndDimensionMap.data[1]])))
#    previousSet = set([l for l in hdStreamClusteringObject.phraseTextAndDimensionMap.data[1]])

experts_twitter_stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'

def movingAverage(list, window): return map(lambda i: np.mean(list[i:i+window]), range(len(list)))
def loadExperimentsData(experimentsData, file):
    for data in FileIO.iterateJsonFromFile(file):
        if data['purity']>0 and data['purity']<1:
            experimentsData[data['iteration_parameters']['type']]['iteration_time'].append(data['iteration_time'])
            experimentsData[data['iteration_parameters']['type']]['quality'].append(data['purity'])
            experimentsData[data['iteration_parameters']['type']]['total_clusters'].append(data['iteration_parameters']['total_clusters'])
def plotClusters(experimentsData, numberOfPoints, pltInfo):
    for k in experimentsData: window = 4; plt.semilogy(range(numberOfPoints)[:-window], movingAverage(experimentsData[k]['total_clusters'][:numberOfPoints], window)[:-window], pltInfo[k]['type'], label=pltInfo[k]['label'], color=pltInfo[k]['color'], lw=2)
    plt.ylabel(getLatexForString('Clusters in memory'))
def plotRunningTime(experimentsData, pltInfo, key1, key2, semilog=True):
    dataY1, dataY2 = [], []
    for y1, y2 in zip(experimentsData[key1]['iteration_time'], experimentsData[key2]['iteration_time']): dataY1.append(y1), dataY2.append(y2)
    numberOfPoints = len(dataY1)
    for k, dataY in zip(experimentsData, [dataY1, dataY2]): 
        window = 20; 
        if semilog: plt.semilogy(range(numberOfPoints)[:-window], movingAverage(dataY[:numberOfPoints], window)[:-window], pltInfo[k]['type'], label=pltInfo[k]['label'], color=pltInfo[k]['color'], lw=2)
        else: plt.plot(range(numberOfPoints)[:-window], movingAverage(dataY[:numberOfPoints], window)[:-window], pltInfo[k]['type'], label=pltInfo[k]['label'], color=pltInfo[k]['color'], lw=2)
    plt.ylabel(getLatexForString('Running time (s)'))
    return numberOfPoints
def plotQuality(experimentsData, numberOfPoints, pltInfo):
    for k in experimentsData: 
        dataY = movingAverage(experimentsData[k]['quality'][:numberOfPoints], 4)
        plt.plot(range(numberOfPoints), [np.mean(dataY)]*numberOfPoints,'--', color=pltInfo[k]['color'], lw=2)
        plt.plot(range(numberOfPoints), dataY, pltInfo[k]['type'], label=pltInfo[k]['label'], color=pltInfo[k]['color'], lw=2)
    plt.ylabel(getLatexForString('Purity'))
    
class Evaluation():
    def __init__(self): self.expertsToClassMap = dict([(k, v['class']) for k,v in getExperts(byScreenName=True).iteritems()])
    def _getExpertClasses(self, cluster): return [self.expertsToClassMap[user.lower()] for user in cluster if user.lower() in self.expertsToClassMap]
    def getEvaluationMetrics(self, documentClusters, timeDifference, iteration_parameters):
        iterationData =  {'iteration_parameters': iteration_parameters, 'no_of_clusters': len(documentClusters), 'iteration_time': timeDifference, 'clusters': documentClusters}
        clustersForEvaluation = [self._getExpertClasses(cluster) for cluster in documentClusters]
        iterationData['nmi'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.nmi)
        iterationData['purity'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.purity)
        iterationData['f1'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.f1)
        return iterationData

evaluation = Evaluation()
previousTime = None

class JustifyDimensionsEstimation():
    first_n_dimension = 'first_n_dimension'
    top_n_dimension = 'top_n_dimension'
    stats_file = clustering_quality_experts_folder+'dimensions_need_analysis'
    def __init__(self): experts_twitter_stream_settings['cluster_filtering_method'] = emptyClusterFilteringMethod
    @staticmethod
    def modifiedClusterAnalysisMethod(hdStreamClusteringObject, currentMessageTime):
        global evaluation, previousTime
        currentTime = time.time()
        documentClusters = [cluster.documentsInCluster.keys() for k, cluster in hdStreamClusteringObject.clusters.iteritems() if len(cluster.documentsInCluster.keys())>=experts_twitter_stream_settings['cluster_filter_threshold']]
        iteration_data = evaluation.getEvaluationMetrics(documentClusters, currentTime-previousTime, {'type': experts_twitter_stream_settings['dimensions_performance_type'], 'dimensions': experts_twitter_stream_settings['dimensions']})
        iteration_data['no_of_observed_dimensions'] = len(hdStreamClusteringObject.phraseTextToPhraseObjectMap)
        previousTime = time.time()
        FileIO.writeToFileAsJson(iteration_data, JustifyDimensionsEstimation.stats_file)
        del iteration_data['clusters']
        print currentMessageTime, iteration_data
        if experts_twitter_stream_settings['dimensions']!=76819 and 2*experts_twitter_stream_settings['dimensions']<=len(hdStreamClusteringObject.phraseTextToPhraseObjectMap): raise Exception
    def generateExperimentData(self):
        global previousTime
        experts_twitter_stream_settings['dimensions_performance_type'] = JustifyDimensionsEstimation.first_n_dimension
        experts_twitter_stream_settings['update_dimensions_method'] = emptyUpdateDimensionsMethod
        experts_twitter_stream_settings['cluster_analysis_method'] = JustifyDimensionsEstimation.modifiedClusterAnalysisMethod
        for dimensions in range(10**4,21*10**4,10**4):
            experts_twitter_stream_settings['dimensions'] = getLargestPrimeLesserThan(dimensions)
            previousTime = time.time()
            try:
                HDStreaminClustering(**experts_twitter_stream_settings).cluster(TwitterIterators.iterateTweetsFromExperts())
            except Exception as e: pass
    def plotJustifyDimensionsEstimation(self):
        runningTimeData, purityData = defaultdict(list), defaultdict(list)
        for data in FileIO.iterateJsonFromFile(JustifyDimensionsEstimation.stats_file):
            if data['iteration_parameters']['dimensions']<data['no_of_observed_dimensions']:
                no_of_dimensions = data['iteration_parameters']['dimensions']
                runningTimeData[no_of_dimensions].append(data['iteration_time']), purityData[no_of_dimensions].append(data['purity'])
#        plt.subplot(211)
#        dataX, dataY = [], []
#        for k in sorted(runningTimeData): dataX.append(k), dataY.append(np.mean(runningTimeData[k])) 
#        plt.semilogx(dataX, dataY, '-x', label=getLatexForString('Fixed dimensions'), color='k', lw=2)
#        plt.ylabel(getLatexForString('Running time (s)'))
#        plt.xlim(7000, 203000)
        
        plt.subplot(111)
        dataX, dataY = [], []
        del purityData[169991]; del purityData[39989]
        plt.title(getLatexForString('Need for dimension estimation'))
        for k in sorted(purityData): dataX.append(k), dataY.append(np.mean(purityData[k])) 
        plt.semilogx(dataX, [0.96]*len(dataX), '--', label=getLatexForString('Top n dimensions'), color='#7109AA', lw=2)
        plt.semilogx(dataX, [np.mean(dataY)]*len(dataX), '--', color='#5AF522', lw=2)
        plt.semilogx(dataX, dataY, '-x', label=getLatexForString('Fixed dimensions'), color='#5AF522', lw=2)
        plt.ylim(0.8, 1.0)
        plt.xlim(7000, 203000)
        plt.xlabel(getLatexForString('\# of dimensions'))
        plt.ylabel(getLatexForString('Purity'))
        plt.legend(loc=3)
        plt.savefig('justifyDimensionsEstimation.pdf')
        
    @staticmethod
    def runExperiment():
#        JustifyDimensionsEstimation().generateExperimentData()
        JustifyDimensionsEstimation().plotJustifyDimensionsEstimation()

class JustifyMemoryPruning:
    with_memory_pruning = 'with_memory_pruning'
    without_memory_pruning = 'without_memory_pruning'
    stats_file = clustering_quality_experts_folder+'memory_pruning_need_analysis'
    @staticmethod
    def modifiedClusterAnalysisMethod(hdStreamClusteringObject, currentMessageTime):
        global evaluation, previousTime
        currentTime = time.time()
        documentClusters = [cluster.documentsInCluster.keys() for k, cluster in hdStreamClusteringObject.clusters.iteritems() if len(cluster.documentsInCluster.keys())>=experts_twitter_stream_settings['cluster_filter_threshold']]
        iteration_data = evaluation.getEvaluationMetrics(documentClusters, currentTime-previousTime, {'type': experts_twitter_stream_settings['pruing_type'], 'total_clusters': len(hdStreamClusteringObject.clusters), 'current_time': getStringRepresentationForTweetTimestamp(currentMessageTime)})
        previousTime = time.time()
        FileIO.writeToFileAsJson(iteration_data, JustifyMemoryPruning.stats_file)
        del iteration_data['clusters']
        print getStringRepresentationForTweetTimestamp(currentMessageTime), iteration_data
    def generateExperimentData(self, withOutPruning):
        global previousTime
        if withOutPruning: experts_twitter_stream_settings['cluster_filtering_method'] = emptyClusterFilteringMethod; experts_twitter_stream_settings['pruing_type'] = JustifyMemoryPruning.without_memory_pruning
        else: experts_twitter_stream_settings['pruing_type'] = JustifyMemoryPruning.with_memory_pruning
        experts_twitter_stream_settings['cluster_analysis_method'] = JustifyMemoryPruning.modifiedClusterAnalysisMethod
        previousTime = time.time()
        HDStreaminClustering(**experts_twitter_stream_settings).cluster(TwitterIterators.iterateTweetsFromExperts(expertsDataStartTime=datetime(2011,3,19), expertsDataEndTime=datetime(2011,3,27)))
    def plotJustifyMemoryPruning(self):
        pltInfo =  {JustifyMemoryPruning.with_memory_pruning: {'label': getLatexForString('With pruning'), 'color': '#7109AA', 'type': '-'}, JustifyMemoryPruning.without_memory_pruning: {'label': getLatexForString('With out pruning'), 'color': '#5AF522', 'type': '-'}}
        experimentsData = {JustifyMemoryPruning.with_memory_pruning: {'iteration_time': [], 'quality': [], 'total_clusters': []}, JustifyMemoryPruning.without_memory_pruning: {'iteration_time': [], 'quality': [], 'total_clusters': []}}
        loadExperimentsData(experimentsData, JustifyMemoryPruning.stats_file)
        numberOfPoints = 275
        plt.subplot(312); plotRunningTime(experimentsData, pltInfo, JustifyMemoryPruning.with_memory_pruning, JustifyMemoryPruning.without_memory_pruning); plt.legend(loc=2, ncol=2); plt.xticks([], tick1On=False), plt.xlim(xmax=270)
        plt.subplot(313); plotQuality(experimentsData, numberOfPoints, pltInfo); plt.xlabel(getLatexForString('Time')), plt.xlim(xmax=270)
        plt.subplot(311); plotClusters(experimentsData, numberOfPoints, pltInfo); plt.title(getLatexForString('Need for memory pruning')); plt.xticks([], tick1On=False), plt.xlim(xmax=270)
        plt.savefig('justifyMemoryPruning.pdf')
    @staticmethod
    def runExperiment():
#        JustifyMemoryPruning().generateExperimentData(withOutPruning=False)
        JustifyMemoryPruning().plotJustifyMemoryPruning()
        
class JustifyExponentialDecay:
    with_decay = 'with_decay'
    without_decay = 'without_decay'
    stats_file = clustering_quality_experts_folder+'exponential_decay_need_analysis'
    @staticmethod
    def modifiedClusterAnalysisMethod(hdStreamClusteringObject, currentMessageTime):
        global evaluation, previousTime
        currentTime = time.time()
        documentClusters = [cluster.documentsInCluster.keys() for k, cluster in hdStreamClusteringObject.clusters.iteritems() if len(cluster.documentsInCluster.keys())>=experts_twitter_stream_settings['cluster_filter_threshold']]
        iteration_data = evaluation.getEvaluationMetrics(documentClusters, currentTime-previousTime, {'type': experts_twitter_stream_settings['decay_type'], 'total_clusters': len(hdStreamClusteringObject.clusters), 'current_time': getStringRepresentationForTweetTimestamp(currentMessageTime)})
        previousTime = time.time()
        FileIO.writeToFileAsJson(iteration_data, JustifyExponentialDecay.stats_file)
        del iteration_data['clusters']
        print getStringRepresentationForTweetTimestamp(currentMessageTime), iteration_data
    def generateExperimentData(self, withOutDecay):
        global previousTime
        if withOutDecay: 
            experts_twitter_stream_settings['decay_type'] = JustifyExponentialDecay.without_decay
            experts_twitter_stream_settings['phrase_decay_coefficient']=1.0; experts_twitter_stream_settings['stream_decay_coefficient']=1.0; experts_twitter_stream_settings['stream_cluster_decay_coefficient']=1.0;
        else: experts_twitter_stream_settings['decay_type'] = JustifyExponentialDecay.with_decay
        experts_twitter_stream_settings['cluster_analysis_method'] = JustifyExponentialDecay.modifiedClusterAnalysisMethod
        previousTime = time.time()
        HDStreaminClustering(**experts_twitter_stream_settings).cluster(TwitterIterators.iterateTweetsFromExperts(expertsDataStartTime=datetime(2011,3,19), expertsDataEndTime=datetime(2011,3,27))) 
    def plotJustifyExponentialDecay(self):
        pltInfo =  {JustifyExponentialDecay.with_decay: {'label': getLatexForString('With decay'), 'color': '#7109AA', 'type': '-'}, JustifyExponentialDecay.without_decay: {'label': getLatexForString('With out decay'), 'color': '#5AF522', 'type': '-'}}
        experimentsData = {JustifyExponentialDecay.with_decay: {'iteration_time': [], 'quality': [], 'total_clusters': []}, JustifyExponentialDecay.without_decay: {'iteration_time': [], 'quality': [], 'total_clusters': []}}
        loadExperimentsData(experimentsData, JustifyExponentialDecay.stats_file)
        plt.subplot(211)
        numberOfPoints = 350
        plotRunningTime(experimentsData, pltInfo, JustifyExponentialDecay.with_decay, JustifyExponentialDecay.without_decay); plt.xticks([], tick1On=False), plt.xlim(xmax=350)
        plt.legend(loc=2)
        plt.title(getLatexForString('Need for exponential decay'))
        plt.ylabel(getLatexForString('Running time (s)'))
        plt.subplot(212)
        plotQuality(experimentsData, numberOfPoints, pltInfo)
        plt.xlabel(getLatexForString('Time'))
        plt.savefig('justifyExponentialDecay.pdf')
    def analyzeJustifyExponentialDecay(self):
        global evaluation
        experimentsData = {JustifyExponentialDecay.with_decay: {}, JustifyExponentialDecay.without_decay: {}}
        for data in FileIO.iterateJsonFromFile(JustifyExponentialDecay.stats_file): experimentsData[data['iteration_parameters']['type']][getDateTimeObjectFromTweetTimestamp(data['iteration_parameters']['current_time'])]=data['clusters']
        qualityData = []
        for k1, k2 in zip(sorted(experimentsData[JustifyExponentialDecay.with_decay]), sorted(experimentsData[JustifyExponentialDecay.without_decay])):
            qualityData.append((k1, evaluation.getEvaluationMetrics(experimentsData[JustifyExponentialDecay.with_decay][k1], None, None)['purity']-evaluation.getEvaluationMetrics(experimentsData[JustifyExponentialDecay.without_decay][k1], None, None)['purity']))
        keyTime = sorted(qualityData, key=itemgetter(1))[-1][0]
        clusterWithDecay = [i for i in experimentsData[JustifyExponentialDecay.with_decay][keyTime] if len(i)>=3]
        clusterWithOutDecay = [i for i in experimentsData[JustifyExponentialDecay.without_decay][keyTime] if len(i)>=3]
#        for c in clusterWithDecay:
#            print c, [evaluation.expertsToClassMap[i.lower()] for i in c]

        interestedCluster = set(['Zap2it', 'ESPNAndyKatz', 'comingsoonnet', '950KJR', 'ginasmith888', 'UKCoachCalipari', 'SportsFanz', 'David_Henrie'])
        for c in clusterWithOutDecay:
            if len(set(c).intersection(interestedCluster))>0: 
#                print c, [evaluation.expertsToClassMap[i.lower()] for i in c]
                setString = ', '.join(['%s (%s)'%(i, evaluation.expertsToClassMap[i.lower()]) for i in sorted(c)]).replace(' ', '\\ ').replace('_', '\\_')
                print keyTime, '&', setString, '\\\\'
            
        clustersDiscoveredEarlierByDecay = {}
        for kt in sorted(experimentsData[JustifyExponentialDecay.with_decay]):
            for c in experimentsData[JustifyExponentialDecay.with_decay][kt]:
                c=sorted(c)
                if len(set(c).intersection(interestedCluster))>0: 
                    classes = [evaluation.expertsToClassMap[i.lower()] for i in c if i.lower() in evaluation.expertsToClassMap]
                    if sorted([(k, len(list(g))/float(len(classes))) for k,g in groupby(sorted(classes))], key=itemgetter(1))[-1][1]>0.7:
                        if kt>datetime(2011,3,19) and kt<=keyTime: clustersDiscoveredEarlierByDecay[kt]=c
        observedStrings = set()
        for k in sorted(clustersDiscoveredEarlierByDecay): 
            setString = ', '.join(['%s (%s)'%(i, evaluation.expertsToClassMap[i.lower()]) for i in sorted(clustersDiscoveredEarlierByDecay[k])]).replace(' ', '\\ ').replace('_', '\\_')
            if setString not in observedStrings: print k, '&', setString, '\\\\'; observedStrings.add(setString)
        
    @staticmethod
    def runExperiment():
#        JustifyExponentialDecay().generateExperimentData(withOutDecay=False)
        JustifyExponentialDecay().plotJustifyExponentialDecay()
#        JustifyExponentialDecay().analyzeJustifyExponentialDecay()

class JustifyTrie:
    with_trie = 'with_trie'
    with_sorted_list = 'with_sorted_list'
    stats_file = clustering_quality_experts_folder+'trie_need_analysis'
    @staticmethod
    def modifiedClusterAnalysisMethod(hdStreamClusteringObject, currentMessageTime):
        global evaluation, previousTime
        currentTime = time.time()
        documentClusters = [cluster.documentsInCluster.keys() for k, cluster in hdStreamClusteringObject.clusters.iteritems() if len(cluster.documentsInCluster.keys())>=experts_twitter_stream_settings['cluster_filter_threshold']]
        iteration_data = evaluation.getEvaluationMetrics(documentClusters, currentTime-previousTime, {'type': experts_twitter_stream_settings['trie_type'], 'total_clusters': len(hdStreamClusteringObject.clusters), 'current_time': getStringRepresentationForTweetTimestamp(currentMessageTime)})
        previousTime = time.time()
        FileIO.writeToFileAsJson(iteration_data, JustifyTrie.stats_file)
        del iteration_data['clusters']
        print getStringRepresentationForTweetTimestamp(currentMessageTime), iteration_data
    def generateExperimentData(self, withoutTrie):
        global previousTime
        if withoutTrie: 
            experts_twitter_stream_settings['trie_type'] = JustifyTrie.with_sorted_list
            experts_twitter_stream_settings['signature_type']='signature_type_list'
        else: experts_twitter_stream_settings['trie_type'] = JustifyTrie.with_trie
        experts_twitter_stream_settings['cluster_analysis_method'] = JustifyTrie.modifiedClusterAnalysisMethod
        experts_twitter_stream_settings['cluster_filtering_method'] = emptyClusterFilteringMethod
        previousTime = time.time()
        HDStreaminClustering(**experts_twitter_stream_settings).cluster(TwitterIterators.iterateTweetsFromExperts(expertsDataStartTime=datetime(2011,3,19), expertsDataEndTime=datetime(2011,3,27))) 
    def plotJustifyTrie(self):
            pltInfo = {JustifyTrie.with_trie: {'label': getLatexForString('With trie'), 'color': '#7109AA', 'type': '-'}, JustifyTrie.with_sorted_list: {'label': getLatexForString('With sorted list'), 'color': '#5AF522', 'type': '-'}}
            experimentsData = {JustifyTrie.with_trie: {'iteration_time': [], 'quality': [], 'total_clusters': []}, JustifyTrie.with_sorted_list: {'iteration_time': [], 'quality': [], 'total_clusters': []}}
            loadExperimentsData(experimentsData, JustifyTrie.stats_file)
            plt.subplot(212); numberOfPoints = plotRunningTime(experimentsData, pltInfo, JustifyTrie.with_trie, JustifyTrie.with_sorted_list); plt.xlim(xmax=200)
            plt.xlabel(getLatexForString('Time'))
            plt.subplot(211); plotClusters(experimentsData, numberOfPoints, pltInfo); plt.xticks([], tick1On=False); plt.xlim(xmax=200)
            plt.title(getLatexForString('Need for trie'))
            plt.legend(loc=4)
            plt.savefig('justifyTrie.pdf')
    @staticmethod
    def runExperiment():
#        JustifyTrie().generateExperimentData(withoutTrie=False)
        JustifyTrie().plotJustifyTrie()
        
class JustifyNotUsingVanillaLSH:
    with_vanilla_lsh = 'with_vanilla_lsh'
    with_modified_lsh = 'with_modified_lsh'
    stats_file = clustering_quality_experts_folder+'modified_lsh_need_analysis'
    @staticmethod
    def modifiedClusterAnalysisMethod(hdStreamClusteringObject, currentMessageTime):
        global evaluation, previousTime
        currentTime = time.time()
        documentClusters = [cluster.documentsInCluster.keys() for k, cluster in hdStreamClusteringObject.clusters.iteritems() if len(cluster.documentsInCluster.keys())>=experts_twitter_stream_settings['cluster_filter_threshold']]
        iteration_data = evaluation.getEvaluationMetrics(documentClusters, currentTime-previousTime, {'type': experts_twitter_stream_settings['lsh_type'], 'total_clusters': len(hdStreamClusteringObject.clusters), 'current_time': getStringRepresentationForTweetTimestamp(currentMessageTime)})
        previousTime = time.time()
        FileIO.writeToFileAsJson(iteration_data, JustifyNotUsingVanillaLSH.stats_file)
        del iteration_data['clusters']
        print getStringRepresentationForTweetTimestamp(currentMessageTime), iteration_data
    def generateExperimentData(self, with_vanilla_lsh):
        global previousTime
        if with_vanilla_lsh: 
            experts_twitter_stream_settings['lsh_type'] = JustifyNotUsingVanillaLSH.with_vanilla_lsh
            experts_twitter_stream_settings['phrase_decay_coefficient']=1.0; experts_twitter_stream_settings['stream_decay_coefficient']=1.0; experts_twitter_stream_settings['stream_cluster_decay_coefficient']=1.0;
            experts_twitter_stream_settings['cluster_filtering_method'] = emptyClusterFilteringMethod;
            experts_twitter_stream_settings['signature_type']='signature_type_list'
#            experts_twitter_stream_settings['dimensions'] = getLargestPrimeLesserThan(100000)
#            experts_twitter_stream_settings['update_dimensions_method'] = emptyUpdateDimensionsMethod
        else: experts_twitter_stream_settings['lsh_type'] = JustifyNotUsingVanillaLSH.with_modified_lsh
        experts_twitter_stream_settings['cluster_analysis_method'] = JustifyNotUsingVanillaLSH.modifiedClusterAnalysisMethod
        previousTime = time.time()
        HDStreaminClustering(**experts_twitter_stream_settings).cluster(TwitterIterators.iterateTweetsFromExperts(expertsDataStartTime=datetime(2011,3,19), expertsDataEndTime=datetime(2011,3,27))) 
    def plotJustifyNotUsingVanillaLSH(self):
            pltInfo = {JustifyNotUsingVanillaLSH.with_modified_lsh: {'label': getLatexForString('Modified LSH'), 'color': '#7109AA', 'type': '-'}, JustifyNotUsingVanillaLSH.with_vanilla_lsh: {'label': getLatexForString('Plain LSH'), 'color': '#5AF522', 'type': '-'}}
            experimentsData = {JustifyNotUsingVanillaLSH.with_modified_lsh: {'iteration_time': [], 'quality': [], 'total_clusters': []}, JustifyNotUsingVanillaLSH.with_vanilla_lsh: {'iteration_time': [], 'quality': [], 'total_clusters': []}}
#            loadExperimentsData(experimentsData, JustifyNotUsingVanillaLSH.stats_file)
            loadExperimentsData(experimentsData, 'temp/modified_lsh_need_analysis')
            plt.subplot(312); numberOfPoints = plotRunningTime(experimentsData, pltInfo, JustifyNotUsingVanillaLSH.with_modified_lsh, JustifyNotUsingVanillaLSH.with_vanilla_lsh, semilog=False); plt.xticks([], tick1On=False)
            plt.legend(loc=2)
            plt.subplot(313); plotQuality(experimentsData, numberOfPoints, pltInfo); plt.xlabel(getLatexForString('Time'))
            plt.subplot(311);plotClusters(experimentsData, numberOfPoints, pltInfo); plt.title(getLatexForString('Need for modified lsh')); plt.xticks([], tick1On=False)
            plt.savefig('justifyNotUsingVanillaLSH.pdf')
    @staticmethod
    def runExperiment():
        JustifyNotUsingVanillaLSH().generateExperimentData(with_vanilla_lsh=False)
#        JustifyNotUsingVanillaLSH().plotJustifyNotUsingVanillaLSH()
    
if __name__ == '__main__':
#    JustifyDimensionsEstimation.runExperiment()
#    JustifyMemoryPruning.runExperiment()
#    JustifyExponentialDecay.runExperiment()
#    JustifyTrie.runExperiment()
    JustifyNotUsingVanillaLSH.runExperiment()
