'''
Created on Jun 23, 2011

@author: kykamath
'''

from collections import defaultdict
from library.nlp import getPhrases, getWordsFromRawEnglishMessage
from library.vector import Vector
from classes import Message
from library.twitter import getDateTimeObjectFromTweetTimestamp

class UtilityMethods:
    @staticmethod
    def getVectorForText(text, phraseToIdMap, max_dimensions, min_phrase_length, max_phrase_length):
        '''
        On observing new phrases in the test:
            If length of phraseToIdMap is lesser than the number of dimensions: Add these new phrases to phraseToIdMap
            Else: Ignore them
        '''
        vectorMap = defaultdict(float)
        for phrase in getPhrases(getWordsFromRawEnglishMessage(text), min_phrase_length, max_phrase_length): 
            if phrase in phraseToIdMap: vectorMap[phraseToIdMap[phrase]]+=1
            elif len(phraseToIdMap)<max_dimensions: 
                phraseToIdMap[phrase]=len(phraseToIdMap)
                vectorMap[phraseToIdMap[phrase]]+=1
        return Vector(vectorMap)
    
    @staticmethod
    def getMessageObjectForTweet(tweet, phraseToIdMap, max_dimensions, min_phrase_length, max_phrase_length):
        message = Message(tweet['user']['screen_name'], tweet['id'], tweet['text'], getDateTimeObjectFromTweetTimestamp(tweet['created_at']))
        message.vector = UtilityMethods.getVectorForText(tweet['text'], phraseToIdMap, max_dimensions, min_phrase_length, max_phrase_length)
        return message