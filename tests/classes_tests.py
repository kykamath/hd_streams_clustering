'''
Created on Jun 22, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
import unittest
from library.nlp import StopWords
from library.vector import Vector
from classes import Stream, Message, VectorUpdateMethods
from datetime import datetime

class StreamTests(unittest.TestCase):
    def setUp(self):
        self.stream = Stream(1, {1:1.,2:3.})
    def test_updateWithVector(self):
        self.stream.updateWithVector(Vector({2:3}), VectorUpdateMethods.addWithoutDecay)
        self.assertEqual(self.stream, Vector({1:1.,2:6.}))

class MessageTests(unittest.TestCase):
    def setUp(self):
        self.message = Message(1, 'sdf', 'A project to cluster high-dimensional streams.', datetime.now())
        StopWords.load()
    def test_getVector(self):
        wordToIdMap = {'project':1, 'cluster': 2, 'highdimensional': 3, 'streams': 4}
        self.assertEqual(Vector({1:1, 2:1, 3:1, 4:1}), self.message.getVector(wordToIdMap, 1, 1))
        
class VectorUpdateMethodTests(unittest.TestCase):
    def setUp(self): 
        self.s1 = Stream(1, {1:2., 2:3.})
        self.v1 = Vector({1:2., 3:3.})
    def test_addWithoutDecay(self):
        VectorUpdateMethods.addWithoutDecay(self.s1, self.v1)
        self.assertEqual(Vector({1: 4, 2: 3, 3: 3}), self.s1)
        
if __name__ == '__main__':
    unittest.main()