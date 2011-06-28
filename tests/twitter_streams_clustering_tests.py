'''
Created on Jun 23, 2011

@author: kykamath
'''
import unittest, sys
sys.path.append('../')
from twitter_streams_clustering import TwitterCrowdsSpecificMethods
from settings import twitter_stream_settings
from datetime import datetime

test_time = datetime.now()
# Settings for unittests
twitter_stream_settings['min_phrase_length']=1
twitter_stream_settings['max_phrase_length']=1

class TwitterCrowdsSpecificMethodsTests(unittest.TestCase):
    def setUp(self):
        self.tweet = {'user':{'screen_name': 'abc'}, 'id':10, 'text':'A project to cluster high-dimensional streams.', 'created_at': 'Tue Mar 01 05:59:59 +0000 2011'}
    def test_getMessageObjectForTweet(self):
        message = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage(self.tweet, **twitter_stream_settings)
        self.assertEqual({'project': 1, 'cluster': 1, 'streams': 1, 'highdimensional': 1}, message.vector)
        
if __name__ == '__main__':
    unittest.main()