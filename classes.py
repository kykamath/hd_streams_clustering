'''
Created on Jun 22, 2011

@author: kykamath
'''

from streaming_lsh.classes import Document
from library.nlp import getPhrases, getWordsFromRawEnglishMessage
from collections import defaultdict
from library.vector import Vector
from library.math_modified import exponentialDecay, DateTimeAirthematic

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

class Message:
    def __init__(self, streamId, messageId, text, timeStamp): 
        self.streamId, self.messageId, self.text, self.timeStamp = streamId, messageId, text, timeStamp
    def setVector(self, wordToIdMap=None, min_phrase_length=None, max_phrase_length=None):
        if 'vector' not in self.__dict__: 
            vectorMap = defaultdict(float)
            for phrase in getPhrases(getWordsFromRawEnglishMessage(self.text), min_phrase_length, max_phrase_length): vectorMap[wordToIdMap[phrase]]+=1
            self.vector = Vector(vectorMap)
