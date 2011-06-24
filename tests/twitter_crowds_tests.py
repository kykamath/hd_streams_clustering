'''
Created on Jun 23, 2011

@author: kykamath
'''
import unittest
from twitter_crowds import TwitterCrowdsSpecificMethods
from library.vector import Vector
from settings import twitter_stream_settings
from classes import Phrase
from datetime import datetime, timedelta

test_time = datetime.now()

class TwitterCrowdsSpecificMethodsTests(unittest.TestCase):
    def setUp(self):
        self.text = 'A project to cluster high-dimensional streams.'
        self.min_phrase_length, self.max_phrase_length, self.max_dimensions = 1, 1, 4
        self.phraseTextToIdMap = {'project':0, 'cluster': 1}
        self.phraseTextToPhraseObjectMap = {'project': Phrase('project', test_time, score=8), 'cluster': Phrase('cluster', test_time, score=8)}
        self.tweet = {'user':{'screen_name': 'abc'}, 'id':10, 'text':self.text, 'created_at': 'Tue Mar 01 05:59:59 +0000 2011'}
        self.vector = Vector({0:1, 1:1, 2:1, 3:1})
    def test_getMessageObjectForTweet(self):
        message = TwitterCrowdsSpecificMethods.getMessageObjectForTweet(self.tweet, self.phraseTextToIdMap, self.phraseTextToPhraseObjectMap, **twitter_stream_settings)
        self.assertEqual(self.vector, message.vector)
        
if __name__ == '__main__':
    unittest.main()