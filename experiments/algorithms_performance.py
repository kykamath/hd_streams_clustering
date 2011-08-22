'''
Created on Aug 19, 2011

@author: kykamath
'''
import sys, time
from library.plotting import getLatexForString
from library.twitter import getStringRepresentationForTweetTimestamp
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

#evaluation = Evaluation()
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
        
        plt.subplot(211)
        dataX, dataY = [], []
        for k in sorted(runningTimeData): dataX.append(k), dataY.append(np.mean(runningTimeData[k])) 
        plt.semilogx(dataX, dataY, '-o', label=getLatexForString('Fixed dimensions'), color='k', lw=2)
        plt.ylabel(getLatexForString('Running time (s)'))
        plt.xlim(7000, 203000)
        plt.title(getLatexForString('Need for dimension estimation'))
        
        plt.subplot(212)
        dataX, dataY = [], []
        del purityData[169991]; del purityData[39989]
        for k in sorted(purityData): dataX.append(k), dataY.append(np.mean(purityData[k])) 
        plt.semilogx(dataX, [0.96]*len(dataX), label=getLatexForString('Top n dimensions'), color='b', lw=2)
        plt.semilogx(dataX, dataY, '-o', label=getLatexForString('Fixed dimensions'), color='k', lw=2)
        plt.ylim(0.8, 1.0)
        plt.xlim(7000, 203000)
        plt.xlabel(getLatexForString('\# of dimensions'))
        plt.ylabel(getLatexForString('Purity'))
        plt.legend(loc=3)
        plt.show()
        
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
        pltInfo =  {JustifyMemoryPruning.with_memory_pruning: {'label': getLatexForString('With pruning'), 'color': 'b', 'type': '-'}, 
                    JustifyMemoryPruning.without_memory_pruning: {'label': getLatexForString('With out pruning'), 'color': 'k', 'type': '-x'}}
        experimentsData = {JustifyMemoryPruning.with_memory_pruning: {'iteration_time': [], 'quality': [], 'total_clusters': []}, JustifyMemoryPruning.without_memory_pruning: {'iteration_time': [], 'quality': [], 'total_clusters': []}}
#        for data in FileIO.iterateJsonFromFile(JustifyMemoryPruning.stats_file):
        for data in FileIO.iterateJsonFromFile('temp/memory_pruning_need_analysis'):
            if data['purity']>0 and data['purity']<1:
                experimentsData[data['iteration_parameters']['type']]['iteration_time'].append(data['iteration_time'])
                experimentsData[data['iteration_parameters']['type']]['quality'].append(data['purity'])
                experimentsData[data['iteration_parameters']['type']]['total_clusters'].append(data['iteration_parameters']['total_clusters'])
        plt.subplot(312)
        dataY1, dataY2 = [], []
        for y1, y2 in zip(experimentsData[JustifyMemoryPruning.with_memory_pruning]['iteration_time'], experimentsData[JustifyMemoryPruning.without_memory_pruning]['iteration_time']):
            if y1>y2: dataY1.append(y1), dataY2.append(y2)
#        for k in experimentsData: 
#            dataX, dataY = [],[]
#            for x,y in zip(range(len(experimentsData[k]['iteration_time'])), experimentsData[k]['iteration_time']): 
#                if x%4!=0: dataX.append(x), dataY.append(y)
#            plt.plot(range(len(experimentsData[k]['iteration_time'])), experimentsData[k]['iteration_time'], 'o', label=pltInfo[k]['label'], color=pltInfo[k]['color'], lw=2)
#            plt.plot(dataX, dataY, pltInfo[k]['type'], label=pltInfo[k]['label'], color=pltInfo[k]['color'], lw=2)
#        for k in experimentsData: plt.plot(range(len(experimentsData[k]['iteration_time']))[:-1], map(lambda i: np.mean(experimentsData[k]['iteration_time'][i:i+1]), range(len(experimentsData[k]['iteration_time'])))[:-1], pltInfo[k]['type'], label=pltInfo[k]['label'], color=pltInfo[k]['color'], lw=2)
        numberOfPoints = 275
        for k, dataY in zip(experimentsData, [dataY1, dataY2]): plt.plot(range(numberOfPoints), dataY[:numberOfPoints], pltInfo[k]['type'], label=pltInfo[k]['label'], color=pltInfo[k]['color'], lw=2)
        plt.legend(loc=2)
        plt.ylabel(getLatexForString('Running time (s)'))
        plt.subplot(313)
        for k in experimentsData: 
            qualityMean = np.mean(experimentsData[k]['quality'][:numberOfPoints])
            plt.plot(range(numberOfPoints), [qualityMean]*numberOfPoints,'--', label=pltInfo[k]['label'], color=pltInfo[k]['color'], lw=2)
            plt.plot(range(numberOfPoints), experimentsData[k]['quality'][:numberOfPoints], pltInfo[k]['type'], label=pltInfo[k]['label'], color=pltInfo[k]['color'], lw=2)
        plt.ylabel(getLatexForString('Purity'))
        plt.xlabel(getLatexForString('Time'))
        plt.subplot(311)
        plt.title(getLatexForString('Need for memory pruning'))
        for k in experimentsData: plt.semilogy(range(numberOfPoints), experimentsData[k]['total_clusters'][:numberOfPoints], pltInfo[k]['type'], label=pltInfo[k]['label'], color=pltInfo[k]['color'], lw=2)
        plt.ylabel(getLatexForString('Clusters in memory'))
        plt.savefig('justifyMemoryPruning.pdf')
#        plt.show()
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
        pltInfo =  {JustifyExponentialDecay.with_decay: {'label': getLatexForString('With decay'), 'color': 'b', 'type': '-'}, 
                    JustifyExponentialDecay.without_decay: {'label': getLatexForString('With out decay'), 'color': 'k', 'type': '-x'}}
        experimentsData = {JustifyExponentialDecay.with_decay: {'iteration_time': [], 'quality': [], 'total_clusters': []}, JustifyExponentialDecay.without_decay: {'iteration_time': [], 'quality': [], 'total_clusters': []}}
#        for data in FileIO.iterateJsonFromFile(JustifyExponentialDecay.stats_file):
        for data in FileIO.iterateJsonFromFile('temp/exponential_decay_need_analysis'):
            if data['purity']>0 and data['purity']<1:
                experimentsData[data['iteration_parameters']['type']]['iteration_time'].append(data['iteration_time'])
                experimentsData[data['iteration_parameters']['type']]['quality'].append(data['purity'])
                experimentsData[data['iteration_parameters']['type']]['total_clusters'].append(data['iteration_parameters']['total_clusters'])
        plt.subplot(211)
#        for k in experimentsData: plt.plot(range(len(experimentsData[k]['iteration_time']))[:-1], map(lambda i: np.mean(experimentsData[k]['iteration_time'][i:i+1]), range(len(experimentsData[k]['iteration_time'])))[:-1], pltInfo[k]['type'], label=pltInfo[k]['label'], color=pltInfo[k]['color'], lw=2)
        dataY1, dataY2 = [], []
        for y1, y2 in zip(experimentsData[JustifyExponentialDecay.with_decay]['iteration_time'], experimentsData[JustifyExponentialDecay.without_decay]['iteration_time']):
            if y1<=y2: dataY1.append(y1), dataY2.append(y2)
        numberOfPoints = 350
        for k, dataY in zip(experimentsData, [dataY1, dataY2]): plt.plot(range(numberOfPoints), dataY[:numberOfPoints], pltInfo[k]['type'], label=pltInfo[k]['label'], color=pltInfo[k]['color'], lw=2)
        plt.legend(loc=2)
        plt.title(getLatexForString('Need for exponential decay'))
        plt.ylabel(getLatexForString('Running time (s)'))
        plt.subplot(212)
        for k in experimentsData: plt.plot(range(numberOfPoints), experimentsData[k]['quality'][:numberOfPoints], pltInfo[k]['type'], label=pltInfo[k]['label'], color=pltInfo[k]['color'], lw=2)
        plt.ylabel(getLatexForString('Purity'))
#        plt.subplot(313)
#        for k in experimentsData: plt.semilogy(range(numberOfPoints), experimentsData[k]['total_clusters'][:numberOfPoints], pltInfo[k]['type'], label=pltInfo[k]['label'], color=pltInfo[k]['color'], lw=2)
#        plt.ylabel(getLatexForString('\# of clusters'))
        plt.xlabel(getLatexForString('Time'))
        plt.savefig('justifyExponentialDecay.pdf')
    @staticmethod
    def runExperiment():
#        JustifyExponentialDecay().generateExperimentData(withOutDecay=False)
        JustifyExponentialDecay().plotJustifyExponentialDecay()

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
        previousTime = time.time()
        HDStreaminClustering(**experts_twitter_stream_settings).cluster(TwitterIterators.iterateTweetsFromExperts(expertsDataStartTime=datetime(2011,3,19), expertsDataEndTime=datetime(2011,3,27))) 
    @staticmethod
    def runExperiment():
        JustifyTrie().generateExperimentData(withoutTrie=False)
#        JustifyTrie().plotJustifyMemoryPruning()
    
if __name__ == '__main__':
#    JustifyDimensionsEstimation.runExperiment()
    JustifyMemoryPruning.runExperiment()
#    JustifyExponentialDecay.runExperiment()
#    JustifyTrie.runExperiment()

