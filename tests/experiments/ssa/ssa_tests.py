'''
Created on Jul 16, 2011

@author: kykamath
'''
import unittest, cjson
from library.vector import Vector
from itertools import combinations
from library.file_io import FileIO
from experiments.ssa.ssa_sim_mr import SSASimilarityMR
from experiments.ssa.ssa import StreamSimilarityAggregationMR

test_file = 'ssa_test.dat'
test_ssa_threshold = 0.75

def createTestFile():
    vectors = {
                    1: Vector({'1':4, '2':8}), 
                    2: Vector({'1':4, '2':8}), 
                    3: Vector({'1':4, '2':8}), 
                    4: Vector({'2':8}), 
                    5: Vector({'3':4, '4':8}), 
                    6: Vector({'4':8}), 
                    7: Vector({'3':4, '4':8}), 
                    8: Vector({'3':4}) 
                }
    with open(test_file, 'w') as f:
        for v1, v2 in combinations(vectors.iteritems(),2): f.write('%s\t%s\n'%(cjson.encode(['x']), cjson.encode([(v1[0], v1[1]), (v2[0], v2[1])])))

class SSASimilarityMRTests(unittest.TestCase):
    def setUp(self):
        self.job = SSASimilarityMR(args='-r local'.split())
    def test_runJob(self):
        testResult = {'stream_in_multiple_clusters': [], 1: {'s': [2, 3, 4], 'cid': None}, 2: {'s': [3, 4], 'cid': None}, 3: {'s': [4], 'cid': None}, 5: {'s': [6, 7], 'cid': None}, 6: {'s': [7], 'cid': None}}
        self.assertEqual(testResult, dict(list(self.job.runJob(inputFileList=[test_file]))))

class StreamSimilarityAggregationMRTests(unittest.TestCase):
    def test_estimate(self):
        self.assertEqual([[1, 2, 3, 4], [5, 6, 7]], list(StreamSimilarityAggregationMR.estimate(test_file)))

if __name__ == '__main__':
    unittest.main()
#    createTestFile()