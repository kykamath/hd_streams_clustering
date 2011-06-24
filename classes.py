'''
Created on Jun 22, 2011

@author: kykamath
'''
from streaming_lsh.classes import Document
from library.math_modified import exponentialDecay, DateTimeAirthematic
from collections import defaultdict
from library.nlp import getPhrases, getWordsFromRawEnglishMessage
from library.vector import Vector
from datetime import timedelta
import random

class UtilityMethods:
    @staticmethod
    def createOrAddNewPhraseObject(phrase, phraseTextToPhraseObjectMap, occuranceTime, **stream_settings):
        if phrase not in phraseTextToPhraseObjectMap: phraseTextToPhraseObjectMap[phrase] = Phrase(phrase, occuranceTime, score=1)
        else: phraseTextToPhraseObjectMap[phrase].updateScore(occuranceTime, scoreToUpdate=1, **stream_settings)
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
    def updateForNewDimensions(phraseTextToIdMap, phraseTextToPhraseObjectMap, currentTime, **stream_settings):
        '''
        Update phraseTextToIdMap with new dimensions.
        '''
        def getNextNewPhrase(topPhrasesSet):
            for phrase in topPhrasesSet: 
                if phrase not in phraseTextToIdMap: yield phrase
        def getNextAvailableId(setOfIds): 
            for id in setOfIds: yield id
        def updatePhraseScore(phraseObject): 
            phraseObject.updateScore(currentTime, 0, **stream_settings)
            return phraseObject
        UtilityMethods.pruneUnnecessaryPhrases(phraseTextToPhraseObjectMap, currentTime, UtilityMethods.pruningConditionDeterministic, **stream_settings)
        topPhrasesList = [p.text for p in Phrase.sort((updatePhraseScore(p) for p in phraseTextToPhraseObjectMap.itervalues()), reverse=True)[:stream_settings['max_dimensions']]]
        topPhrasesSet = set(topPhrasesList)
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
    @staticmethod
    def pruneUnnecessaryPhrases(phraseTextToPhraseObjectMap, currentTime, pruningMethod, **stream_settings):
        def prune(phraseText): 
            if pruningMethod(phraseTextToPhraseObjectMap[phraseText], currentTime, **stream_settings): del phraseTextToPhraseObjectMap[phraseText]
        map(prune, phraseTextToPhraseObjectMap.keys()[:])
    @staticmethod
    def pruningConditionDeterministic(phraseObject, currentTime, **stream_settings):
        if currentTime-phraseObject.latestOccuranceTime>stream_settings['max_phrase_inactivity_time_in_seconds']: return True
        else: return False
    @staticmethod
    def pruningConditionRandom(phraseObject, currentTime, **stream_settings):
        def flip(p): return True if random.random() < p else False
        timeDifference = currentTime-phraseObject.latestOccuranceTime
        if timeDifference<stream_settings['max_phrase_inactivity_time_in_seconds']: return False
        elif timeDifference>2*stream_settings['max_phrase_inactivity_time_in_seconds']: return True
        else: return flip(float(timeDifference.seconds)/(2*stream_settings['max_phrase_inactivity_time_in_seconds']).seconds)
    

class VectorUpdateMethods:
    @staticmethod
    def addWithoutDecay(stream, vector, **kwargs): 
        for k,v in vector.iteritems():
            if k in stream: stream[k]+=v
            else: stream[k]=v
    @staticmethod
    def exponentialDecay(stream, vector, decayCoefficient, timeDifference, **kwargs):
        for k in stream: stream[k]=exponentialDecay(stream[k], decayCoefficient, timeDifference)
        for k in vector: 
            if k in stream: stream[k]+=vector[k]
            else: stream[k]=vector[k]
        
class Stream(Document):
    def __init__(self, streamId, message): 
        super(Stream, self).__init__(streamId, message.vector)
        self.lastMessageTime = message.timeStamp
    def updateForMessage(self, message, updateMethod, **stream_settings): 
        timeDifference = None
        if stream_settings['time_unit_in_seconds']!=None: timeDifference = DateTimeAirthematic.getDifferenceInTimeUnits(message.timeStamp, self.lastMessageTime, stream_settings['time_unit_in_seconds'].seconds)
        updateMethod(self, message.vector, decayCoefficient=stream_settings['stream_decay_coefficient'], timeDifference=timeDifference)

class Message(object):
    def __init__(self, streamId, messageId, text, timeStamp): self.streamId, self.messageId, self.text, self.timeStamp, self.vector = streamId, messageId, text, timeStamp, None
    def __str__(self): return str(self.messageId)

class Phrase:
    def __init__(self, text, latestOccuranceTime, score=1): self.text, self.latestOccuranceTime, self.score = text, latestOccuranceTime, score
    def updateScore(self, currentOccuranceTime, scoreToUpdate, **stream_settings):
        timeDifference = DateTimeAirthematic.getDifferenceInTimeUnits(currentOccuranceTime, self.latestOccuranceTime, stream_settings['time_unit_in_seconds'].seconds)
        self.score=exponentialDecay(self.score, stream_settings['phrase_decay_coefficient'], timeDifference)+scoreToUpdate
        self.latestOccuranceTime=currentOccuranceTime
    @staticmethod
    def sort(phraseIterator, reverse=False): return sorted(phraseIterator, key=lambda phrase:phrase.score, reverse=reverse)
    