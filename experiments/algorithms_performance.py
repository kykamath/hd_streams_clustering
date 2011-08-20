'''
Created on Aug 19, 2011

@author: kykamath
'''
import sys, time
sys.path.append('../')
from settings import experts_twitter_stream_settings
from hd_streams_clustering import HDStreaminClustering
from twitter_streams_clustering import TwitterIterators,\
    TwitterCrowdsSpecificMethods, getExperts
from library.math_modified import getLargestPrimeLesserThan
from library.clustering import EvaluationMetrics
from library.file_io import FileIO
from datetime import datetime

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

class DimensionsPerformance():
    stats_file = clustering_quality_experts_folder+'dimensions_need_analysis'
    def __init__(self):
        experts_twitter_stream_settings['update_dimensions_method'] = emptyUpdateDimensionsMethod
        experts_twitter_stream_settings['cluster_analysis_method'] = DimensionsPerformance.modifiedClusterAnalysisMethod
        experts_twitter_stream_settings['cluster_filtering_method'] = emptyClusterFilteringMethod
        
    @staticmethod
    def modifiedClusterAnalysisMethod(hdStreamClusteringObject, currentMessageTime):
        global evaluation, previousTime
        currentTime = time.time()
        documentClusters = [cluster.documentsInCluster.keys() for k, cluster in hdStreamClusteringObject.clusters.iteritems() if len(cluster.documentsInCluster.keys())>=experts_twitter_stream_settings['cluster_filter_threshold']]
        iteration_data = evaluation.getEvaluationMetrics(documentClusters, currentTime-previousTime, {'dimensions': experts_twitter_stream_settings['dimensions']})
        iteration_data['no_of_observed_dimensions'] = len(hdStreamClusteringObject.phraseTextAndDimensionMap)
        previousTime = time.time()
        FileIO.writeToFileAsJson(iteration_data, 
                                  DimensionsPerformance.stats_file)
        del iteration_data['clusters']
        print currentMessageTime, iteration_data
    
    def runExperiment(self):
        global previousTime
        for dimensions in range(10**4,201*10**4,10**4):
            experts_twitter_stream_settings['dimensions'] = getLargestPrimeLesserThan(dimensions)
            previousTime = time.time()
            HDStreaminClustering(**experts_twitter_stream_settings).cluster(TwitterIterators.iterateTweetsFromExperts(expertsDataStartTime=datetime(2011,3,19), expertsDataEndTime=datetime(2011,3,20)))
        
if __name__ == '__main__':
    DimensionsPerformance().runExperiment()
