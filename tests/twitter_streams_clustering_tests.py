'''
Created on Jun 23, 2011

@author: kykamath
'''
import unittest, sys
from streaming_lsh.classes import Cluster, Document
from library.vector import Vector
sys.path.append('../')
from twitter_streams_clustering import TwitterCrowdsSpecificMethods
from settings import twitter_stream_settings
from datetime import datetime

test_time = datetime.now()
# Settings for unittests
twitter_stream_settings['min_phrase_length']=1
twitter_stream_settings['max_phrase_length']=1
twitter_stream_settings['cluster_merging_jaccard_distance_threshold']=0.3

class TwitterCrowdsSpecificMethodsTests(unittest.TestCase):
    def setUp(self):
        self.tweet = {'user':{'screen_name': 'abc'}, 'id':10, 'text':'A project to cluster high-dimensional streams.', 'created_at': 'Tue Mar 01 05:59:59 +0000 2011'}
    def test_convertTweetJSONToMessage(self):
        message = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage(self.tweet, **twitter_stream_settings)
        self.assertEqual({'project': 1, 'cluster': 1, 'streams': 1, 'highdimensional': 1}, message.vector)
    def test_combineClusters(self):
        cluster1 = Cluster(Vector({'#tcot':2,'dsf':4}))
        cluster2 = Cluster(Vector({'#tcot':4}))
        doc1 = Document(1, Vector({'#tcot':2}))
        doc2 = Document(2, Vector({'#tcot':2}))
        meanVectorForAllDocuments = Vector.getMeanVector([cluster1, cluster2, doc1, doc2])
        cluster1.addDocument(doc1)
        cluster2.addDocument(doc2)
        mergedCluster = Cluster.getClusterObjectToMergeFrom(cluster1)
        mergedCluster.mergeCluster(cluster2)
        
        clustersMap = {cluster1.clusterId: cluster1, cluster2.clusterId: cluster2}
        clustersMap = TwitterCrowdsSpecificMethods.combineClusters(clustersMap, **twitter_stream_settings)
        self.assertEqual(1, len(clustersMap))
        mergedCluster = clustersMap.values()[0]
        self.assertEqual([doc1, doc2], list(mergedCluster.iterateDocumentsInCluster()))
        self.assertEqual(meanVectorForAllDocuments, mergedCluster)
        self.assertEqual([mergedCluster.docId, mergedCluster.docId], list(doc.clusterId for doc in mergedCluster.iterateDocumentsInCluster()))
        
if __name__ == '__main__':
    unittest.main()