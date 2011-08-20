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
    
    @staticmethod
    def modifiedClusterAnalysisMethod(hdStreamClusteringObject, currentMessageTime):
        global evaluation, previousTime
        currentTime = time.time()
        documentClusters = [cluster.documentsInCluster.keys() for k, cluster in hdStreamClusteringObject.clusters.iteritems() if len(cluster.documentsInCluster.keys())>=experts_twitter_stream_settings['cluster_filter_threshold']]
        iteration_data = evaluation.getEvaluationMetrics(documentClusters, currentTime-previousTime, {'type': experts_twitter_stream_settings['pruing_type'], 'number_of_clusters': len(hdStreamClusteringObject.clusters)})
        previousTime = time.time()
#        FileIO.writeToFileAsJson(iteration_data, JustifyDimensionsEstimation.stats_file)
        del iteration_data['clusters']
        print getStringRepresentationForTweetTimestamp(currentMessageTime), iteration_data
    
    def generateExperimentData(self, withOutPruning):
        global previousTime
        if withOutPruning: experts_twitter_stream_settings['cluster_filtering_method'] = emptyClusterFilteringMethod; experts_twitter_stream_settings['pruing_type'] = JustifyMemoryPruning.without_memory_pruning
        else: experts_twitter_stream_settings['pruing_type'] = JustifyMemoryPruning.with_memory_pruning
        experts_twitter_stream_settings['cluster_analysis_method'] = JustifyMemoryPruning.modifiedClusterAnalysisMethod
        previousTime = time.time()
        try:
            HDStreaminClustering(**experts_twitter_stream_settings).cluster(TwitterIterators.iterateTweetsFromExperts())
        except Exception as e: pass
    @staticmethod
    def runExperiment():
        JustifyMemoryPruning().generateExperimentData(withOutPruning=False)
    
if __name__ == '__main__':
#    JustifyDimensionsEstimation.runExperiment()
    JustifyMemoryPruning.runExperiment()
