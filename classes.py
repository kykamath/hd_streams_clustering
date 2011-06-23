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
    '''
    Only the methods in this class have access to settings.
    '''
    @staticmethod
    def getVectorForText(text, occuranceTime, phraseTextToIdMap, idToPhraseObjectMap, **kwargs):
        '''
        On observing a new phrase in the text:
            If length of phraseTextToIdMap is lesser than the number of dimensions: 
                Add the new phrases to phraseTextToIdMap
                Add the phrase to idToPhraseObjectMap
            Else: Ignore it
        Update the phraseObject scores for all known dimensions
        '''
        vectorMap = defaultdict(float)
        for phrase in getPhrases(getWordsFromRawEnglishMessage(text), kwargs['min_phrase_length'], kwargs['max_phrase_length']): 
            if phrase in phraseTextToIdMap: 
                id = phraseTextToIdMap[phrase]
                vectorMap[id]+=1
                idToPhraseObjectMap[id].updateScore(occuranceTime, kwargs['phrase_decay_coefficient'], kwargs['time_unit_in_seconds'], scoreToUpdate=vectorMap[id])
            elif len(phraseTextToIdMap)<kwargs['max_dimensions']: 
                phraseId = len(phraseTextToIdMap)
                phraseTextToIdMap[phrase]=phraseId
                vectorMap[phraseId]+=1
                idToPhraseObjectMap[phraseId] = Phrase(phrase, occuranceTime, score=1)
        return Vector(vectorMap)
    
class Phrase:
    def __init__(self, text, latestOccuranceTime, score=1): self.text, self.latestOccuranceTime, self.score = text, latestOccuranceTime, score
    def updateScore(self, currentOccuranceTime, decayCoefficient, timeUnitInSeconds, scoreToUpdate):
        timeDifference = DateTimeAirthematic.getDifferenceInTimeUnits(currentOccuranceTime, self.latestOccuranceTime, timeUnitInSeconds)
        self.score=exponentialDecay(self.score, decayCoefficient, timeDifference)+scoreToUpdate
        self.latestOccuranceTime=currentOccuranceTime
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
