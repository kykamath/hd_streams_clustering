'''
Created on Jul 2, 2011

@author: kykamath
'''
import unittest
from twitter_streams_clustering import TwitterCrowdsSpecificMethods
from experiments.experts_twitter_stream_analysis import AnalyzeData
from datetime import datetime, timedelta

test_time = datetime.now()

class AnalyzeDataTests(unittest.TestCase):
    def setUp(self):
        AnalyzeData.crowdMap, AnalyzeData.clusterIdToCrowdIdMap, AnalyzeData.crowdIdToClusterIdMap = {}, {}, {}
        self.clusterMaps = {
                       test_time: [
                            {'clusterId': 'cluster_4', 'lastStreamAddedTime':test_time, 'mergedClustersList': ['cluster_1'], 'streams': [], 'dimensions': {}},
                            {'clusterId': 'cluster_5', 'lastStreamAddedTime':test_time, 'mergedClustersList': ['cluster_2'], 'streams': [], 'dimensions': {}},
                            {'clusterId': 'cluster_6', 'lastStreamAddedTime':test_time, 'mergedClustersList': ['cluster_3'], 'streams': [], 'dimensions': {}},
                            ],
                       test_time+timedelta(seconds=30*60): [
                            {'clusterId': 'cluster_7', 'lastStreamAddedTime':test_time, 'mergedClustersList': ['cluster_4'], 'streams': [], 'dimensions': {}},
                            {'clusterId': 'cluster_8', 'lastStreamAddedTime':test_time, 'mergedClustersList': ['cluster_5','cluster_6'], 'streams': [], 'dimensions': {}},
                            ],
                        test_time+2*timedelta(seconds=30*60): [
                            {'clusterId': 'cluster_9', 'lastStreamAddedTime':test_time, 'mergedClustersList': ['cluster_7'], 'streams': [], 'dimensions': {}},
                            {'clusterId': 'cluster_10', 'lastStreamAddedTime':test_time, 'mergedClustersList': ['cluster_8'], 'streams': [], 'dimensions': {}},
                            ]
                       }
        AnalyzeData.constructCrowdDataStructures(self.dataIterator)
        
    def dataIterator(self):
        for currentTime, clusterMaps in sorted(self.clusterMaps.iteritems()):
            for clusterMap in clusterMaps: yield (currentTime, TwitterCrowdsSpecificMethods.getClusterFromMapFormat(clusterMap))
    def test_constructCrowdDataStructures(self):
        self.assertEqual({'cluster_8': 'cluster_2', 'cluster_9': 'cluster_1', 'cluster_6': 'cluster_3', 'cluster_7': 'cluster_1', 'cluster_4': 'cluster_1', 'cluster_5': 'cluster_2', 'cluster_10': 'cluster_2'},
                          AnalyzeData.clusterIdToCrowdIdMap)
        self.assertEqual({'cluster_2': ['cluster_8', 'cluster_5', 'cluster_10'], 'cluster_3': ['cluster_6'], 'cluster_1': ['cluster_9', 'cluster_7', 'cluster_4']}, 
                         AnalyzeData.crowdIdToClusterIdMap)
    def test_getCrowdHierarchy(self):
        self.assertEqual({'cluster_6': 'cluster_8', 'cluster_5': 'cluster_8', 'cluster_2': 'cluster_5', 'cluster_3': 'cluster_6', 'cluster_8': 'cluster_10'},
                         AnalyzeData.getCrowdHierarchy('cluster_6'))
        self.assertEqual({'cluster_6': 'cluster_8', 'cluster_5': 'cluster_8', 'cluster_2': 'cluster_5', 'cluster_3': 'cluster_6', 'cluster_8': 'cluster_10'},
                         AnalyzeData.getCrowdHierarchy('cluster_10'))
if __name__ == '__main__':
    unittest.main()