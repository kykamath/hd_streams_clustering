'''
Created on Jun 22, 2011

@author: kykamath
'''
import unittest
from library.nlp import getPhrases, getWordsFromRawEnglishMessage
from library.vector import Vector
from classes import Stream, Message, VectorUpdateMethods, UtilityMethods, Phrase
from datetime import datetime, timedelta
from settings import twitter_stream_settings

test_time = datetime.now()

# Settings for unittests
twitter_stream_settings['phrase_decay_coefficient'] = 0.5
twitter_stream_settings['time_unit_in_seconds'] = 60

class UtilityMethodsTests(unittest.TestCase):
    def setUp(self):
        self.text = 'A project to cluster high-dimensional streams.'
        self.phraseToIdMap = {'project':0, 'cluster': 1}
        self.idToPhraseObjectMap = {0: Phrase('project', test_time, score=8), 1: Phrase('cluster', test_time, score=8)}
        self.vector = Vector({0:1, 1:1, 2:1, 3:1})
    def test_getVectorForString_PhraseMapHasLesserDimensions(self):
        self.assertEqual(['project', 'cluster', 'highdimensional', 'streams'], getPhrases(getWordsFromRawEnglishMessage(self.text), 1, 1))
        self.assertEqual(self.vector, UtilityMethods.getVectorForText(self.text, test_time, self.phraseToIdMap, self.idToPhraseObjectMap, **twitter_stream_settings))
        self.assertEqual({'project':0, 'cluster': 1, 'highdimensional':2, 'streams': 3}, self.phraseToIdMap)
    def test_getVectorForString_PhraseMapHasMaximumDimensions(self):
        tempVal = twitter_stream_settings['max_dimensions']
        twitter_stream_settings['max_dimensions'] = 2
        self.assertEqual(Vector({0:1, 1:1}), UtilityMethods.getVectorForText(self.text, test_time, self.phraseToIdMap, self.idToPhraseObjectMap, **twitter_stream_settings))
        self.assertEqual({'project':0, 'cluster': 1}, self.phraseToIdMap)
        twitter_stream_settings['max_dimensions'] = tempVal
    def test_getVectorForString_PhraseObjectIsCreated(self): 
        UtilityMethods.getVectorForText(self.text, test_time, self.phraseToIdMap, self.idToPhraseObjectMap, **twitter_stream_settings)
        self.assertEqual(4, len(self.idToPhraseObjectMap))
    def test_getVectorForString_PhraseObjectScoresAreUpdatedCorrectly(self): 
        UtilityMethods.getVectorForText(self.text, test_time+timedelta(seconds=60), self.phraseToIdMap, self.idToPhraseObjectMap, **twitter_stream_settings)
        self.assertEqual(5, self.idToPhraseObjectMap[0].score) # Score for 'project'
        self.assertEqual(1, self.idToPhraseObjectMap[3].score) # Score for 'streams'
        pass
    # Test if scores are getting updated. Test if phrase object is created

class StreamTests(unittest.TestCase):
    def setUp(self):
        self.m1 = Message(1, 'sdf', 'A project to cluster high-dimensional streams.', test_time-timedelta(seconds=60))
        self.m1.vector=Vector({1:1.,2:3.})
        self.stream = Stream(1, self.m1)
        self.m2 = Message(1, 'sdf', 'A project to cluster high-dimensional streams.', test_time)
        self.m2.vector=Vector({2:3.})
    def test_updateForMessage_addWithoutDecay(self):
        self.stream.updateForMessage(self.m2, VectorUpdateMethods.addWithoutDecay, None, None)
        self.assertEqual(self.stream, Vector({1:1.,2:6.}))
    def test_updateForMessage_exponentialDecay(self):
        self.stream.updateForMessage(self.m2, VectorUpdateMethods.exponentialDecay, 0.5, twitter_stream_settings['time_unit_in_seconds'])
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
        
class PhraseTests(unittest.TestCase):
    def setUp(self):
        self.phrase1 = Phrase('abc', test_time, score=8)
        self.phrase2 = Phrase('xyz', test_time, score=7)
    def test_updateScore(self):
        self.phrase1.updateScore(test_time+timedelta(seconds=120), 0.5, twitter_stream_settings['time_unit_in_seconds'], 0)
        self.assertEqual(2, self.phrase1.score)
        self.assertEqual(test_time+timedelta(seconds=120), self.phrase1.latestOccuranceTime)
    def test_sort(self):
        self.assertEqual([self.phrase2, self.phrase1], Phrase.sort([self.phrase1, self.phrase2]))
        self.assertEqual([self.phrase1, self.phrase2], Phrase.sort([self.phrase1, self.phrase2], reverse=True))
            
        
if __name__ == '__main__':
    unittest.main()
