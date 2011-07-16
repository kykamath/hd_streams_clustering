'''
Created on Jul 16, 2011

@author: kykamath
'''
import sys
sys.path.append('../../')
import unittest
from library.vector import Vector
from experiments.quality_comparison_with_similar_stream_aggregation import SimilarStreamAggregation,\
    ItemsClusterer


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
    
if __name__ == '__main__':
    unittest.main()
