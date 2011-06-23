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
    def createOrAddNewPhraseObject(phrase, phraseTextToPhraseObjectMap, occuranceTime, **stream_settings):
        if phrase not in phraseTextToPhraseObjectMap: phraseTextToPhraseObjectMap[phrase] = Phrase(phrase, occuranceTime, score=1)
        else: phraseTextToPhraseObjectMap[phrase].updateScore(occuranceTime, stream_settings['phrase_decay_coefficient'], stream_settings['time_unit_in_seconds'], scoreToUpdate=1)
    @staticmethod
    def getVectorForText(text, occuranceTime, phraseTextToIdMap, phraseTextToPhraseObjectMap, **stream_settings):
        '''
        On observing a new phrase in the text:
            If length of phraseTextToIdMap is lesser than the number of dimensions: 
                Add the new phrases to phraseTextToIdMap
            Else: Ignore it
        For every phrase:
            If the phrase is not found in phraseTextToPhraseObjectMap: Add the phrase to phraseTextToPhraseObjectMap
            Else: Update the score for that phrase 
        '''
        vectorMap = defaultdict(float)
        for phrase in getPhrases(getWordsFromRawEnglishMessage(text), stream_settings['min_phrase_length'], stream_settings['max_phrase_length']): 
            phraseId = None
            if phrase in phraseTextToIdMap: phraseId=phraseTextToIdMap[phrase]
            elif len(phraseTextToIdMap)<stream_settings['max_dimensions']: phraseId=len(phraseTextToIdMap); phraseTextToIdMap[phrase]=phraseId
            if phraseId!=None: vectorMap[phraseId]+=1
            UtilityMethods.createOrAddNewPhraseObject(phrase, phraseTextToPhraseObjectMap, occuranceTime, **stream_settings)
        return Vector(vectorMap)
    @staticmethod
    def updateForNewDimensions(phraseTextToIdMap, phraseTextToPhraseObjectMap, **stream_settings):
        '''
        Update phraseTextToIdMap with new dimensions.
        '''
        def getNextNewPhrase(topPhrasesSet):
            for phrase in topPhrasesSet: 
                if phrase not in phraseTextToIdMap: yield phrase
        def getNextAvailableId(setOfIds): 
            for id in setOfIds: yield id
        topPhrasesSet = set([p.text for p in Phrase.sort(phraseTextToPhraseObjectMap.itervalues(), reverse=True)[:stream_settings['max_dimensions']]])
        newPhraseIterator = getNextNewPhrase(topPhrasesSet)
        availableIds = set(list(range(stream_settings['max_dimensions'])))
        for phrase in phraseTextToIdMap.keys()[:]:
            availableIds.remove(phraseTextToIdMap[phrase])
            if phrase not in topPhrasesSet:
                newPhrase = newPhraseIterator.next()
                phraseTextToIdMap[newPhrase]=phraseTextToIdMap[phrase]
                del phraseTextToIdMap[phrase] 
        availableIdsIterator = getNextAvailableId(availableIds)
        while True: 
            try:
                phraseTextToIdMap[newPhraseIterator.next()] = availableIdsIterator.next()
            except StopIteration: break
        # Check critical mistakes with the run. Stop application here.
        UtilityMethods.checkCriticalErrorsInPhraseTextToIdMap(phraseTextToIdMap, **stream_settings)
    @staticmethod
    def checkCriticalErrorsInPhraseTextToIdMap(phraseTextToIdMap, **stream_settings):
        if len(phraseTextToIdMap)>stream_settings['max_dimensions']: print 'Illegal number of dimensions.', exit()
        if len(phraseTextToIdMap.values())!=len(set(phraseTextToIdMap.values())): print 'Multiple phrases with same id.', exit()

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
