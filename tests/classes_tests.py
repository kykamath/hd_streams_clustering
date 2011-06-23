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
        self.phraseTextToIdMap = {'project':0, 'cluster': 1}
        self.phraseTextToPhraseObjectMap = {'project': Phrase('project', test_time, score=8), 'cluster': Phrase('cluster', test_time, score=8)}
        self.vector = Vector({0:1, 1:1, 2:1, 3:1})
        self.initial_max_dimensions = twitter_stream_settings['max_dimensions']
    def tearDown(self):  twitter_stream_settings['max_dimensions'] = self.initial_max_dimensions
    def test_getVectorForString_PhraseMapHasLesserDimensions(self):
        self.assertEqual(['project', 'cluster', 'highdimensional', 'streams'], getPhrases(getWordsFromRawEnglishMessage(self.text), 1, 1))
        self.assertEqual(self.vector, UtilityMethods.getVectorForText(self.text, test_time, self.phraseTextToIdMap, self.phraseTextToPhraseObjectMap, **twitter_stream_settings))
        self.assertEqual({'project':0, 'cluster': 1, 'highdimensional':2, 'streams': 3}, self.phraseTextToIdMap)
    def test_getVectorForString_PhraseMapHasMaximumDimensions(self):
        twitter_stream_settings['max_dimensions'] = 2
        self.assertEqual(Vector({0:1, 1:1}), UtilityMethods.getVectorForText(self.text, test_time, self.phraseTextToIdMap, self.phraseTextToPhraseObjectMap, **twitter_stream_settings))
        self.assertEqual({'project':0, 'cluster': 1}, self.phraseTextToIdMap)
#        twitter_stream_settings['max_dimensions'] = tempVal
    def test_getVectorForString_PhraseObjectScoresAreUpdatedCorrectly(self): 
        UtilityMethods.getVectorForText(self.text, test_time+timedelta(seconds=60), self.phraseTextToIdMap, self.phraseTextToPhraseObjectMap, **twitter_stream_settings)
        self.assertEqual(4, len(self.phraseTextToPhraseObjectMap))
        self.assertEqual(5, self.phraseTextToPhraseObjectMap['project'].score)
        self.assertEqual(1, self.phraseTextToPhraseObjectMap['streams'].score)
    def test_getVectorForString_phrase_does_not_exist_in_phraseToIdMap_but_exists_in_phraseTextToPhraseObjectMap_with_dimensions_full(self): 
        # Fill dimensions.
        twitter_stream_settings['max_dimensions'] = 1
        # Delete a dimension for phraseTextToIdMap so that phraseTextToPhraseObjectMap has extra object
        del self.phraseTextToIdMap['cluster']
        UtilityMethods.getVectorForText(self.text, test_time+timedelta(seconds=60), self.phraseTextToIdMap, self.phraseTextToPhraseObjectMap, **twitter_stream_settings)
        self.assertEqual({'project':0}, self.phraseTextToIdMap)
        self.assertEqual(4, len(self.phraseTextToPhraseObjectMap))
        self.assertEqual(5, self.phraseTextToPhraseObjectMap['project'].score)
        self.assertEqual(5, self.phraseTextToPhraseObjectMap['cluster'].score)
        self.assertEqual(1, self.phraseTextToPhraseObjectMap['streams'].score)
    def test_createOrAddNewPhraseObject(self):
        UtilityMethods.createOrAddNewPhraseObject('new_phrase', self.phraseTextToPhraseObjectMap, test_time, **twitter_stream_settings)
        UtilityMethods.createOrAddNewPhraseObject('project', self.phraseTextToPhraseObjectMap, test_time, **twitter_stream_settings)
        self.assertEqual(3, len(self.phraseTextToPhraseObjectMap))
        self.assertEqual(1, self.phraseTextToPhraseObjectMap['new_phrase'].score)
        self.assertEqual(9, self.phraseTextToPhraseObjectMap['project'].score)
    def test_updateForNewDimensions_when_phraseTextToIdMap_is_filled_to_max_dimensions(self):
        twitter_stream_settings['max_dimensions'] = 2
        for phrase, score in zip(['added'], range(10,11)): self.phraseTextToPhraseObjectMap[phrase] = Phrase(phrase, test_time, score=score)
        UtilityMethods.updateForNewDimensions(self.phraseTextToIdMap, self.phraseTextToPhraseObjectMap, **twitter_stream_settings)
        self.assertEqual({'project':0, 'added': 1}, self.phraseTextToIdMap)
    def test_updateForNewDimensions_when_phraseTextToIdMap_is_filled_to_max_dimensions_and_entire_map_is_changed(self):
        twitter_stream_settings['max_dimensions'] = 2
        for phrase, score in zip(['added', 'are'], range(10,12)): self.phraseTextToPhraseObjectMap[phrase] = Phrase(phrase, test_time, score=score)
        UtilityMethods.updateForNewDimensions(self.phraseTextToIdMap, self.phraseTextToPhraseObjectMap, **twitter_stream_settings)
        self.assertEqual({'added':0, 'are': 1}, self.phraseTextToIdMap)
    def test_updateForNewDimensions_when_phraseTextToIdMap_has_lesser_than_max_dimensions(self):
        twitter_stream_settings['max_dimensions'] = 4
        for phrase, score in zip(['new', 'phrases', 'are', 'added'], range(7,11)): self.phraseTextToPhraseObjectMap[phrase] = Phrase(phrase, test_time, score=score)
        UtilityMethods.updateForNewDimensions(self.phraseTextToIdMap, self.phraseTextToPhraseObjectMap, **twitter_stream_settings)
        self.assertEqual(set({'project':0, 'phrases': 1, 'are':2, 'added':3}), set(self.phraseTextToIdMap))
        self.assertEqual(4, len(self.phraseTextToIdMap))
    def test_updateForNewDimensions_when_phrases_with_lower_id_are_removed_from_phraseTextToIdMap(self):
        twitter_stream_settings['max_dimensions'] = 3
        for phrase, score in zip(['new', 'phrases', 'are'], range(100,103)): self.phraseTextToPhraseObjectMap[phrase] = Phrase(phrase, test_time, score=score)
        self.phraseTextToIdMap['cluster']=2
        self.phraseTextToPhraseObjectMap['cluster'].score=100
        UtilityMethods.updateForNewDimensions(self.phraseTextToIdMap, self.phraseTextToPhraseObjectMap, **twitter_stream_settings)
        self.assertEqual(range(3), sorted(self.phraseTextToIdMap.values()))
    def test_checkCriticalErrorsInPhraseTextToIdMap_larger_than_expected_dimensions(self):
        twitter_stream_settings['max_dimensions'] = 2
        self.phraseTextToIdMap['sdfsd']=0
        print 'Ignore this message: ',
        self.assertRaises(SystemExit, UtilityMethods.checkCriticalErrorsInPhraseTextToIdMap, self.phraseTextToIdMap, **twitter_stream_settings)
    def test_checkCriticalErrorsInPhraseTextToIdMap_repeating_values(self):
        twitter_stream_settings['max_dimensions'] = 2    
        self.phraseTextToIdMap['cluster']=0
        print 'Ignore this message: ',
        self.assertRaises(SystemExit, UtilityMethods.checkCriticalErrorsInPhraseTextToIdMap, self.phraseTextToIdMap, **twitter_stream_settings)
        
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
