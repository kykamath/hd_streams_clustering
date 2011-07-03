'''
Created on Jun 22, 2011

@author: kykamath
'''
import unittest
from library.vector import Vector
from classes import Stream, Message, VectorUpdateMethods, UtilityMethods, Phrase,\
    Crowd, StreamCluster
from datetime import datetime, timedelta
from settings import twitter_stream_settings as stream_settings
from library.classes import TwoWayMap, GeneralMethods
from streaming_lsh.classes import Cluster
from library.twitter import getStringRepresentationForTweetTimestamp

test_time = datetime.now()

# Settings for unittests
stream_settings['min_phrase_length']=1
stream_settings['max_phrase_length']=1
stream_settings['phrase_decay_coefficient'] = 0.5
stream_settings['time_unit_in_seconds'] = timedelta(seconds=60)
stream_settings['stream_decay_coefficient'] = 0.5
stream_settings['max_phrase_inactivity_time_in_seconds'] = timedelta(seconds=60)

class UtilityMethodsTests(unittest.TestCase):
    def setUp(self):
        self.phraseVector = {'project':1, 'cluster':1, 'highdimensional':1, 'streams':1}
        self.phraseTextAndDimensionMap = TwoWayMap()
        self.phraseTextAndDimensionMap.set(TwoWayMap.MAP_FORWARD, 'project', 0)
        self.phraseTextAndDimensionMap.set(TwoWayMap.MAP_FORWARD, 'cluster', 1)
        self.phraseTextToPhraseObjectMap = {'project': Phrase('project', test_time, score=8), 'cluster': Phrase('cluster', test_time, score=8), 'abcd': Phrase('abcd', test_time-3*stream_settings['max_phrase_inactivity_time_in_seconds'], score=8)}
        self.vector = Vector({0:1, 1:1, 2:1, 3:1})
        self.initial_max_dimensions = stream_settings['max_dimensions']
        stream_settings['max_dimensions'] = 2
    def tearDown(self): stream_settings['max_dimensions'] = self.initial_max_dimensions
    def test_updatedPhraseObject_PhraseObjectScoresAreUpdatedCorrectly(self): 
        UtilityMethods.updatePhraseTextToPhraseObject(self.phraseVector, test_time+timedelta(seconds=60), self.phraseTextToPhraseObjectMap, **stream_settings)
        self.assertEqual(5, len(self.phraseTextToPhraseObjectMap))
        self.assertEqual(5, self.phraseTextToPhraseObjectMap['project'].score)
        self.assertEqual(1, self.phraseTextToPhraseObjectMap['streams'].score)
    
    def test_updatedPhraseObject_phrase_does_not_exist_in_phraseToIdMap_but_exists_in_phraseTextToPhraseObjectMap_with_dimensions_full(self): 
        stream_settings['max_dimensions'] = 1
        self.phraseTextAndDimensionMap.remove(TwoWayMap.MAP_FORWARD, 'cluster')
        UtilityMethods.updatePhraseTextToPhraseObject(self.phraseVector, test_time+timedelta(seconds=60), self.phraseTextToPhraseObjectMap, **stream_settings)
        self.assertEqual({'project':0}, self.phraseTextAndDimensionMap.getMap(TwoWayMap.MAP_FORWARD))
        self.assertEqual(5, len(self.phraseTextToPhraseObjectMap))
        self.assertEqual(5, self.phraseTextToPhraseObjectMap['project'].score)
        self.assertEqual(5, self.phraseTextToPhraseObjectMap['cluster'].score)
        self.assertEqual(1, self.phraseTextToPhraseObjectMap['streams'].score)

    def test_createOrAddNewPhraseObject(self):
        UtilityMethods.createOrAddNewPhraseObject('new_phrase', self.phraseTextToPhraseObjectMap, test_time, **stream_settings)
        UtilityMethods.createOrAddNewPhraseObject('project', self.phraseTextToPhraseObjectMap, test_time, **stream_settings)
        self.assertEqual(4, len(self.phraseTextToPhraseObjectMap))
        self.assertEqual(1, self.phraseTextToPhraseObjectMap['new_phrase'].score)
        self.assertEqual(9, self.phraseTextToPhraseObjectMap['project'].score)
    
    def test_updateDimensions_when_phraseTextToIdMap_is_filled_to_max_dimensions(self):
        for phrase, score in zip(['added'], range(10,11)): self.phraseTextToPhraseObjectMap[phrase] = Phrase(phrase, test_time, score=score)
        UtilityMethods.updateDimensions(self.phraseTextAndDimensionMap, self.phraseTextToPhraseObjectMap, test_time, **stream_settings)
        self.assertEqual({'project':0, 'added': 1}, self.phraseTextAndDimensionMap.getMap(TwoWayMap.MAP_FORWARD))
    
    def test_updateDimensions_when_phraseTextToIdMap_is_filled_to_max_dimensions_and_entire_map_is_changed(self):
        for phrase, score in zip(['added', 'are'], range(10,12)): self.phraseTextToPhraseObjectMap[phrase] = Phrase(phrase, test_time, score=score)
        UtilityMethods.updateDimensions(self.phraseTextAndDimensionMap, self.phraseTextToPhraseObjectMap, test_time, **stream_settings)
        self.assertEqual({'added':1, 'are': 0}, self.phraseTextAndDimensionMap.getMap(TwoWayMap.MAP_FORWARD))
    
    def test_updateDimensions_when_phraseTextToIdMap_has_lesser_than_max_dimensions(self):
        stream_settings['max_dimensions'] = 4
        for phrase, score in zip(['new', 'phrases', 'are', 'added'], range(7,11)): self.phraseTextToPhraseObjectMap[phrase] = Phrase(phrase, test_time, score=score)
        UtilityMethods.updateDimensions(self.phraseTextAndDimensionMap, self.phraseTextToPhraseObjectMap, test_time, **stream_settings)
        self.assertEqual(set({'project':0, 'phrases': 1, 'are':2, 'added':3}), set(self.phraseTextAndDimensionMap.getMap(TwoWayMap.MAP_FORWARD)))
        self.assertEqual(4, len(self.phraseTextAndDimensionMap))
    
    def test_updateDimensions_when_phrases_with_lower_id_are_removed_from_phraseTextToIdMap(self):
        stream_settings['max_dimensions'] = 3
        for phrase, score in zip(['new', 'phrases', 'are'], range(100,103)): self.phraseTextToPhraseObjectMap[phrase] = Phrase(phrase, test_time, score=score)
        self.phraseTextAndDimensionMap.set(TwoWayMap.MAP_FORWARD, 'cluster', 2)
        self.phraseTextToPhraseObjectMap['cluster'].score=100
        UtilityMethods.updateDimensions(self.phraseTextAndDimensionMap, self.phraseTextToPhraseObjectMap, test_time, **stream_settings)
        self.assertEqual(range(3), sorted(self.phraseTextAndDimensionMap.getMap(TwoWayMap.MAP_FORWARD).values()))
    
    def test_updateDimensions_remove_old_phrases(self):
        originalTime=self.phraseTextToPhraseObjectMap['abcd'].latestOccuranceTime
        self.phraseTextToPhraseObjectMap['abcd'].latestOccuranceTime=test_time
        UtilityMethods.updateDimensions(self.phraseTextAndDimensionMap, self.phraseTextToPhraseObjectMap, test_time, **stream_settings)
        self.assertTrue('abcd' in self.phraseTextToPhraseObjectMap)
        self.phraseTextToPhraseObjectMap['abcd'].latestOccuranceTime=originalTime
        UtilityMethods.updateDimensions(self.phraseTextAndDimensionMap, self.phraseTextToPhraseObjectMap, test_time, **stream_settings)
        self.assertTrue('abcd' not in self.phraseTextToPhraseObjectMap)
    
    
    def test_updateDimensions_when_dimensions_have_to_be_removed(self):
        stream_settings['max_dimensions'] = 4
        self.phraseTextAndDimensionMap.set(TwoWayMap.MAP_FORWARD, 'abcdx', 2)
        self.phraseTextAndDimensionMap.set(TwoWayMap.MAP_FORWARD, 'abcdxy', 3)
        for phrase, score in zip(['new_text'], range(7,8)): self.phraseTextToPhraseObjectMap[phrase] = Phrase(phrase, test_time, score=score)
        self.phraseTextToPhraseObjectMap['cluster'].latestOccuranceTime=test_time-3*stream_settings['max_phrase_inactivity_time_in_seconds']
        UtilityMethods.updateDimensions(self.phraseTextAndDimensionMap, self.phraseTextToPhraseObjectMap, test_time, **stream_settings)
        self.assertEqual(set({'project':0, 'new_text': 1}), set(self.phraseTextAndDimensionMap.getMap(TwoWayMap.MAP_FORWARD)))
    
    def test_checkCriticalErrorsInPhraseTextToIdMap_larger_than_expected_dimensions(self):
        self.phraseTextAndDimensionMap.set(TwoWayMap.MAP_FORWARD, 'sdfsd', 3)
        print 'Ignore this message: ',
        self.assertRaises(SystemExit, UtilityMethods.checkCriticalErrorsInPhraseTextToIdMap, self.phraseTextAndDimensionMap, **stream_settings)
    
    def test_pruningConditionDeterministic(self):
        phrase1 = Phrase('dsf', test_time-3*stream_settings['max_phrase_inactivity_time_in_seconds'], 1)
        phrase2 = Phrase('dsf', test_time, 1)
        self.assertTrue(UtilityMethods.pruningConditionDeterministic(phrase1, test_time, **stream_settings))
        self.assertFalse(UtilityMethods.pruningConditionDeterministic(phrase2, test_time, **stream_settings))
    
    def test_pruningConditionRandom(self):
        phrase1 = Phrase('dsf', test_time-3*stream_settings['max_phrase_inactivity_time_in_seconds'], 1)
        phrase2 = Phrase('dsf', test_time, 1)
        self.assertTrue(UtilityMethods.pruningConditionRandom(phrase1, test_time, **stream_settings))
        self.assertFalse(UtilityMethods.pruningConditionRandom(phrase2, test_time, **stream_settings))
    
    def test_pruneUnnecessaryPhrases(self):
        phraseTextToPhraseObjectMap = {'dsf': Phrase('dsf', test_time-3*stream_settings['max_phrase_inactivity_time_in_seconds'], 1), 'abc': Phrase('abc', test_time, 1)}
        UtilityMethods.pruneUnnecessaryPhrases(phraseTextToPhraseObjectMap, test_time, UtilityMethods.pruningConditionRandom, **stream_settings)
        self.assertTrue('dsf' not in phraseTextToPhraseObjectMap)
        self.assertTrue('abc' in phraseTextToPhraseObjectMap)
        
class StreamTests(unittest.TestCase):
    def setUp(self):
        self.m1 = Message(1, 'sdf', 'A project to cluster high-dimensional streams.', test_time-timedelta(seconds=60))
        self.m1.vector=Vector({1:1.,2:3.})
        self.stream = Stream(1, self.m1)
        self.m2 = Message(1, 'sdf', 'A project to cluster high-dimensional streams.', test_time)
        self.m2.vector=Vector({2:3.})
    def test_updateForMessage_addWithoutDecay(self):
        self.stream.updateForMessage(self.m2, VectorUpdateMethods.addWithoutDecay, **stream_settings)
        self.assertEqual(self.stream, Vector({1:1.,2:6.}))
    def test_updateForMessage_exponentialDecay(self):
        self.stream.updateForMessage(self.m2, VectorUpdateMethods.exponentialDecay, **stream_settings)
        self.assertEqual(self.stream, Vector({1:0.5,2:4.5}))
    def test_check_lastMessageTime_is_updated(self):
        self.assertEqual(test_time-timedelta(seconds=60), self.stream.lastMessageTime)
        self.stream.updateForMessage(self.m2, VectorUpdateMethods.addWithoutDecay, **stream_settings)
        self.assertNotEqual(test_time-timedelta(seconds=60), self.stream.lastMessageTime)
        self.assertEqual(test_time, self.stream.lastMessageTime)
        
class StreamClusterTests(unittest.TestCase):
    def setUp(self): 
        self.m1 = Message(1, 'sdf', 'A project to cluster high-dimensional streams.', test_time-timedelta(seconds=60))
        self.m1.vector=Vector({1:2,2:4})
        self.stream1 = Stream(1, self.m1)
        self.m2 = Message(2, 'sdf', 'A project to cluster high-dimensional streams.', test_time)
        self.m2.vector=Vector({2:4})
        self.stream2 = Stream(2, self.m2)
        self.cluster1 = StreamCluster(self.stream1)
        self.cluster2 = StreamCluster(self.stream2)
    def test_initialization(self):
        self.assertEqual(test_time-timedelta(seconds=60), self.cluster1.lastStreamAddedTime)
        self.assertEqual(test_time, self.cluster2.lastStreamAddedTime)
        self.assertTrue(1==self.cluster1.score and 1==self.cluster2.score)
    def test_addDocument(self):
        message1 = Message(3, 'sdf', 'A project to cluster high-dimensional streams.', test_time)
        message1.vector=Vector({3:4})
        stream1 = Stream(3, message1)
        message2 = Message(4, 'sdf', 'A project to cluster high-dimensional streams.', test_time)
        message2.vector=Vector({2:4})
        stream2 = Stream(4, message2)
        self.assertEqual(1, self.cluster1.score)
        self.cluster1.addDocument(stream1, **stream_settings)
        self.assertEqual(1.5, self.cluster1.score)
        # Test if cluster id is set.
        self.assertEqual(self.cluster1.clusterId, stream1.clusterId)
        # Test that cluster mean is updated.
        self.assertEqual({1:2/2.,2:2.,3:2.}, self.cluster1)
        # Test that cluster aggrefate is updated.
        self.assertEqual({1:2,2:4,3:4}, self.cluster1.aggregateVector)
        # Test that document is added to cluster documents.
        self.assertEqual(stream1, self.cluster1.documentsInCluster[stream1.docId])
        self.cluster1.addDocument(stream2, **stream_settings)
        self.assertEqual(2.5, self.cluster1.score)
        self.assertEqual(3, self.cluster1.vectorWeights)
        self.assertEqual({1:2/3.,2:8/3.,3:4/3.}, self.cluster1)
        self.assertEqual({1:2,2:8,3:4}, self.cluster1.aggregateVector)
        self.cluster2.addDocument(stream2, **stream_settings)
        self.assertEqual(2, self.cluster2.score)

class CrowdTests(unittest.TestCase):
    def setUp(self):
        self.cluster = Cluster({})
        self.crowd = Crowd(self.cluster, test_time)
    def test_intitialization(self):
        self.assertEqual(self.cluster.clusterId, self.crowd.crowdId)
    def test_append(self):
        cluster2 = Cluster({})
        self.crowd.append(cluster2, test_time+timedelta(days=1))
        self.assertEqual([GeneralMethods.getEpochFromDateTimeObject(test_time), GeneralMethods.getEpochFromDateTimeObject(test_time+timedelta(days=1))], sorted(self.crowd.clusters.keys()))
        self.assertEqual(Cluster, type(self.crowd.clusters[GeneralMethods.getEpochFromDateTimeObject(test_time)]))
        self.assertEqual(2, self.crowd.lifespan)
        self.assertEqual(getStringRepresentationForTweetTimestamp(test_time), getStringRepresentationForTweetTimestamp(self.crowd.startTime))
        self.assertEqual(getStringRepresentationForTweetTimestamp(test_time+timedelta(days=1)), getStringRepresentationForTweetTimestamp(self.crowd.endTime))
        
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
        self.phrase1.updateScore(test_time+timedelta(seconds=120), 0, **stream_settings)
        self.assertEqual(2, self.phrase1.score)
        self.assertEqual(test_time+timedelta(seconds=120), self.phrase1.latestOccuranceTime)
    def test_sort(self):
        self.assertEqual([self.phrase2, self.phrase1], Phrase.sort([self.phrase1, self.phrase2]))
        self.assertEqual([self.phrase1, self.phrase2], Phrase.sort([self.phrase1, self.phrase2], reverse=True))
        
if __name__ == '__main__':
    unittest.main()
