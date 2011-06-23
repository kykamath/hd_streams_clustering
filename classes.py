'''
Created on Jun 22, 2011

@author: kykamath
'''

from streaming_lsh.classes import Document
from library.math_modified import exponentialDecay, DateTimeAirthematic
from collections import defaultdict
from library.nlp import getPhrases, getWordsFromRawEnglishMessage
from library.vector import Vector

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
    
class Phrase:
    def __init__(self, text, latestOccuranceTime, score=1): self.text, self.latestOccuranceTime, self.score = text, latestOccuranceTime, score
    def updateScore(self, currentOccuranceTime, decayCoefficient, timeUnitInSeconds, scoreToUpdate=1):
        timeDifference = DateTimeAirthematic.getDifferenceInTimeUnits(currentOccuranceTime, self.latestOccuranceTime, timeUnitInSeconds)
        self.score=exponentialDecay(self.score, decayCoefficient, timeDifference)+scoreToUpdate
    @staticmethod
    def sort(phraseIterator, reverse=False): return sorted(phraseIterator, key=lambda phrase:phrase.score, reverse=reverse)
    
class VectorUpdateMethods:
    @staticmethod
    def addWithoutDecay(stream, vector, **kwargs): 
        for k,v in vector.iteritems():
            if k in stream: stream[k]+=v
            else: stream[k]=v
    @staticmethod
    def exponentialDecay(stream, vector, decayCoefficient, timeDifference):
        for k in stream: stream[k]=exponentialDecay(stream[k], decayCoefficient, timeDifference)
        for k in vector: 
            if k in stream: stream[k]+=vector[k]
            else: stream[k]=vector[k]
        
class Stream(Document):
    def __init__(self, streamId, message): 
        super(Stream, self).__init__(streamId, message.vector)
        self.lastMessageTime = message.timeStamp
    def updateForMessage(self, message, updateMethod, decayCoefficient, timeUnitInSeconds): 
        timeDifference = None
        if timeUnitInSeconds!=None: timeDifference = DateTimeAirthematic.getDifferenceInTimeUnits(message.timeStamp, self.lastMessageTime, timeUnitInSeconds)
        updateMethod(self, message.vector, decayCoefficient=decayCoefficient, timeDifference=timeDifference)

class Message(object):
    def __init__(self, streamId, messageId, text, timeStamp): 
        self.streamId, self.messageId, self.text, self.timeStamp, self.vector = streamId, messageId, text, timeStamp, None
    def __str__(self): return str(self.messageId)
