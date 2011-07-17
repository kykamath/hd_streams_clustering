'''
Created on Jul 16, 2011

@author: kykamath
'''
import sys, os, unittest, cjson
sys.path.append('../../../')
from library.vector import Vector
from itertools import combinations
from experiments.ssa.ssa_sim_mr import SSASimilarityMR
from experiments.ssa.ssa import StreamSimilarityAggregationMR, ItemsClusterer,\
    SimilarStreamAggregation

test_file = 'ssa_test.dat.gz'
test_ssa_threshold = 0.75

def createTestFile():
    vectors = {
                    '1': Vector({'1':4, '2':8}), 
                    '2': Vector({'1':4, '2':8}), 
                    '3': Vector({'1':4, '2':8}), 
                    '4': Vector({'2':8}), 
                    '5': Vector({'3':4, '4':8}), 
                    '6': Vector({'4':8}), 
                    '7': Vector({'3':4, '4':8}), 
                    '8': Vector({'3':4}) 
                }
    with open(test_file, 'w') as f:
        for v1, v2 in combinations(vectors.iteritems(),2): f.write('%s\t%s\n'%(cjson.encode(['x']), cjson.encode([(v1[0], v1[1]), (v2[0], v2[1])])))

class ItemsClustererTests(unittest.TestCase):
    def setUp(self): self.clusterer = ItemsClusterer()
    def test_addNewCluster(self):
        items = set([1,2,3])
        self.clusterer.add(items)
        self.assertEqual([items], [cluster for cluster in self.clusterer.iterateClusters()])
        self.assertEqual(1, len(set([self.clusterer.itemToClusterMap[i] for i in items])))
    def test_addItemsToExistingCluster(self):
        items = set([1,2,3])
        newItems = set([1,4,5])
        self.clusterer.add(items)
        self.clusterer.add(newItems)
        self.assertEqual([items.union(newItems)], [cluster for cluster in self.clusterer.iterateClusters()])
        self.assertEqual(1, len(set([self.clusterer.itemToClusterMap[i] for i in items.union(newItems)])))
    def test_addItemsWithMultiplePossibleClusters(self):
        items1 = set([1,2,3])
        items2 = set([6,4,5])
        items3 = set([7, 1, 6])
        self.clusterer.add(items1), self.clusterer.add(items2), self.clusterer.add(items3)
        self.assertEqual(1, len(list(self.clusterer.iterateClusters())))
        self.assertEqual(7, len(list(self.clusterer.iterateClusters())[0]))
        self.assertEqual(7, len(self.clusterer.itemToClusterMap))
        self.assertEqual(1, len(set([self.clusterer.itemToClusterMap[i] for i in items1.union(items2).union(items3)])))

class SimilarStreamAggregationTests(unittest.TestCase):
    def setUp(self):
        self.vectors = {
                            1: Vector({1:4, 2:8}), 
                            2: Vector({1:4, 2:8}),
                            3: Vector({1:4, 2:8}), 
                            4: Vector({2:8}),
                            5: Vector({3:4, 4:8}), 
                            6: Vector({4:8}),
                            7: Vector({3:4, 4:8}), 
                            8: Vector({3:4})
                        }
    def test_estimate(self):
        nn = SimilarStreamAggregation(self.vectors, 0.99)
        nn.estimate()
        self.assertEqual(2, len(list(nn.iterateClusters())))

class SSASimilarityMRTests(unittest.TestCase):
    def setUp(self):
        self.job = SSASimilarityMR(args='-r local'.split())
    def test_runJob(self):
        testResult = {'stream_in_multiple_clusters': [], '1': {'s': ['2', '3', '4'], 'cid': None}, '3': {'s': ['4'], 'cid': None}, '2': {'s': ['3', '4'], 'cid': None}, '5': {'s': ['6', '7'], 'cid': None}, '6': {'s': ['7'], 'cid': None}}
        self.assertEqual(testResult, dict(list(self.job.runJob(inputFileList=[test_file]))))

class StreamSimilarityAggregationMRTests(unittest.TestCase):
    def test_estimate(self):
        args = '-r hadoop' if os.uname()[1]=='spock' else '-r local'
        self.assertEqual([['1', '3', '2', '4'], ['5', '7', '6']], list(StreamSimilarityAggregationMR.estimate(test_file, args.split(), jobconf={'mapred.reduce.tasks':2})))

if __name__ == '__main__':
    unittest.main()
#    createTestFile()