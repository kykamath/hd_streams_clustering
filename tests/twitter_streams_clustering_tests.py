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
        self.cluster1 = Cluster(Vector({'#tcot':2,'dsf':4}))
        self.cluster2 = Cluster(Vector({'#tcot':4}))
        self.doc1 = Document(1, Vector({'#tcot':2}))
        self.doc2 = Document(2, Vector({'#tcot':2}))
        self.meanVectorForAllDocuments = Vector.getMeanVector([self.cluster1, self.cluster2, self.doc1, self.doc2])
        self.cluster1.addDocument(self.doc1)
        self.cluster2.addDocument(self.doc2)
    def test_convertTweetJSONToMessage(self):
        message = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage(self.tweet, **twitter_stream_settings)
        self.assertEqual({'project': 1, 'cluster': 1, 'streams': 1, 'highdimensional': 1}, message.vector)
    def test_combineClusters(self):
        clustersMap = {self.cluster1.clusterId: self.cluster1, self.cluster2.clusterId: self.cluster2}
        clustersMap = TwitterCrowdsSpecificMethods.combineClusters(clustersMap, **twitter_stream_settings)
        self.assertEqual(1, len(clustersMap))
        mergedCluster = clustersMap.values()[0]
        self.assertEqual([self.doc1, self.doc2], list(mergedCluster.iterateDocumentsInCluster()))
        self.assertEqual(self.meanVectorForAllDocuments, mergedCluster)
        self.assertEqual([mergedCluster.docId, mergedCluster.docId], list(doc.clusterId for doc in mergedCluster.iterateDocumentsInCluster()))
        self.assertEqual([self.cluster1.clusterId, self.cluster2.clusterId], mergedCluster.mergedClustersList)
    def test_getClusterInMapFormat(self):
        mergedCluster = Cluster.getClusterObjectToMergeFrom(self.cluster1)
        mergedCluster.mergedClustersList = [self.cluster1.clusterId]
        mapReresentation = {'clusterId': mergedCluster.clusterId, 'mergedClustersList': [self.cluster1.clusterId], 'streams': [self.doc1.docId], 'dimensions': {'#tcot':2, 'dsf':2}}
        self.assertEqual(mapReresentation, TwitterCrowdsSpecificMethods.getClusterInMapFormat(mergedCluster))
        
if __name__ == '__main__':
    unittest.main()