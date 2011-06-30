'''
Created on Jun 30, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from settings import experts_twitter_stream_settings
from twitter_streams_clustering import TwitterCrowdsSpecificMethods,\
    TwitterIterators
from hd_streams_clustering import HDStreaminClustering
from streaming_lsh.classes import Cluster
from operator import itemgetter

class GenerateData:
    @staticmethod
    def expertClusters():
        def analyzeIterationData(hdStreamClusteringObject, currentMessageTime):
            print '\n\n\nEntering:', currentMessageTime, len(hdStreamClusteringObject.phraseTextAndDimensionMap), len(hdStreamClusteringObject.phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
            for cluster, _ in sorted(Cluster.iterateByAttribute(hdStreamClusteringObject.clusters.values(), 'length'), key=itemgetter(1), reverse=True)[:1]:
                print TwitterCrowdsSpecificMethods.getClusterInMapFormat(cluster)
            print 'Leaving: ', currentMessageTime, len(hdStreamClusteringObject.phraseTextAndDimensionMap), len(hdStreamClusteringObject.phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
        
        experts_twitter_stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
        experts_twitter_stream_settings['combine_clusters_method'] = TwitterCrowdsSpecificMethods.combineClusters
        experts_twitter_stream_settings['analyze_iteration_data_method'] = analyzeIterationData
        hdsClustering = HDStreaminClustering(**experts_twitter_stream_settings)
        hdsClustering.cluster(TwitterIterators.iterateTweetsFromExperts())
        
if __name__ == '__main__':
    GenerateData.expertClusters()