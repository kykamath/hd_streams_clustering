'''
Created on Jun 23, 2011

@author: kykamath
'''
import sys, unittest
from twitter_classes import UtilityMethods
sys.path.append('../')
from library.nlp import StopWords, getPhrases, getWordsFromRawEnglishMessage
from library.vector import Vector

class UtilityMethodsTests(unittest.TestCase):
    def setUp(self):
        self.tweetText = 'A project to cluster high-dimensional streams.'
        self.min_phrase_length, self.max_phrase_length, self.max_dimensions = 1, 1, 4
        self.phraseToIdMap = {'project':0, 'cluster': 1}
        self.tweet = {'user':{'screen_name': 'abc'}, 'id':10, 'text':self.tweetText, 'created_at': 'Tue Mar 01 05:59:59 +0000 2011'}
        self.tweetVector = Vector({0:1, 1:1, 2:1, 3:1})
        StopWords.load()
    def test_getVectorForString_PhraseMapHasLesserDimensions(self):
        self.assertEqual(['project', 'cluster', 'highdimensional', 'streams'], getPhrases(getWordsFromRawEnglishMessage(self.tweetText), 1, 1))
        self.assertEqual(self.tweetVector, UtilityMethods.getVectorForText(self.tweetText, self.phraseToIdMap, self.max_dimensions, self.min_phrase_length, self.max_phrase_length))
        self.assertEqual({'project':0, 'cluster': 1, 'highdimensional':2, 'streams': 3}, self.phraseToIdMap)
    def test_getVectorForString_PhraseMapHasMaximumDimensions(self):
        self.max_dimensions = 2
        self.assertEqual(Vector({0:1, 1:1}), UtilityMethods.getVectorForText(self.tweetText, self.phraseToIdMap, self.max_dimensions, self.min_phrase_length, self.max_phrase_length))
        self.assertEqual({'project':0, 'cluster': 1}, self.phraseToIdMap)
    def test_getMessageObjectForTweet(self):
        message = UtilityMethods.getMessageObjectForTweet(self.tweet, self.phraseToIdMap, self.max_dimensions, self.min_phrase_length, self.max_phrase_length)
        self.assertEqual(self.tweetVector, message.vector)
        
if __name__ == '__main__':
    unittest.main()