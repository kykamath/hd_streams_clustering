'''
Created on Jun 22, 2011

@author: kykamath
'''

from streaming_lsh.classes import Document
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

class Message(object):
    def __init__(self, streamId, messageId, text, timeStamp): 
        self.streamId, self.messageId, self.text, self.timeStamp, self.vector = streamId, messageId, text, timeStamp, None
#    def setVector(self, wordToIdMap=None, min_phrase_length=None, max_phrase_length=None):
#        if 'vector' not in self.__dict__: 
#            vectorMap = defaultdict(float)
#            for phrase in getPhrases(getWordsFromRawEnglishMessage(self.text), min_phrase_length, max_phrase_length): 
#                if phrase in wordToIdMap: vectorMap[wordToIdMap[phrase]]+=1
#            self.vector = Vector(vectorMap)
    def __str__(self): return str(self.messageId)
