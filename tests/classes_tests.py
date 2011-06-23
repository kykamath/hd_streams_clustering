'''
Created on Jun 22, 2011

@author: kykamath
'''
import unittest
from library.nlp import getPhrases, getWordsFromRawEnglishMessage
from library.vector import Vector
from classes import Stream, Message, VectorUpdateMethods, UtilityMethods
from datetime import datetime, timedelta

class UtilityMethodsTests(unittest.TestCase):
    def setUp(self):
        self.text = 'A project to cluster high-dimensional streams.'
        self.min_phrase_length, self.max_phrase_length, self.max_dimensions = 1, 1, 4
        self.phraseToIdMap = {'project':0, 'cluster': 1}
        self.vector = Vector({0:1, 1:1, 2:1, 3:1})
    def test_getVectorForString_PhraseMapHasLesserDimensions(self):
        self.assertEqual(['project', 'cluster', 'highdimensional', 'streams'], getPhrases(getWordsFromRawEnglishMessage(self.text), 1, 1))
        self.assertEqual(self.vector, UtilityMethods.getVectorForText(self.text, self.phraseToIdMap, self.max_dimensions, self.min_phrase_length, self.max_phrase_length))
        self.assertEqual({'project':0, 'cluster': 1, 'highdimensional':2, 'streams': 3}, self.phraseToIdMap)
    def test_getVectorForString_PhraseMapHasMaximumDimensions(self):
        self.max_dimensions = 2
        self.assertEqual(Vector({0:1, 1:1}), UtilityMethods.getVectorForText(self.text, self.phraseToIdMap, self.max_dimensions, self.min_phrase_length, self.max_phrase_length))
        self.assertEqual({'project':0, 'cluster': 1}, self.phraseToIdMap)

class StreamTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.now()
        self.m1 = Message(1, 'sdf', 'A project to cluster high-dimensional streams.', self.now-timedelta(seconds=60))
        self.m1.vector=Vector({1:1.,2:3.})
        self.stream = Stream(1, self.m1)
        self.m2 = Message(1, 'sdf', 'A project to cluster high-dimensional streams.', self.now)
        self.m2.vector=Vector({2:3.})
    def test_updateForMessage_addWithoutDecay(self):
        self.stream.updateForMessage(self.m2, VectorUpdateMethods.addWithoutDecay, None, None)
        self.assertEqual(self.stream, Vector({1:1.,2:6.}))
    def test_updateForMessage_exponentialDecay(self):
        self.stream.updateForMessage(self.m2, VectorUpdateMethods.exponentialDecay, 0.5, 60)
        self.assertEqual(self.stream, Vector({1:0.5,2:4.5}))
        
class VectorUpdateMethodTests(unittest.TestCase):
    def setUp(self): 
        self.message = Message(1, 'sdf', 'A project to cluster high-dimensional streams.', datetime.now())
        self.message.vector = Vector({1:2., 2:3.})
        self.s1 = Stream(1, self.message)
        self.v1 = Vector({1:2., 3:3.})
    def test_addWithoutDecay(self):
        VectorUpdateMethods.addWithoutDecay(self.s1, self.v1)
        self.assertEqual(Vector({1: 4, 2: 3, 3: 3}), self.s1)
    def test_exponentialDecay(self):
        VectorUpdateMethods.exponentialDecay(self.s1, self.v1, 0.5, 1)
        self.assertEqual(Vector({1: 3, 2: 1.5, 3: 3}), self.s1)
        
if __name__ == '__main__':
    unittest.main()