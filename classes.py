'''
Created on Jun 22, 2011

@author: kykamath
'''
import random
from streaming_lsh.classes import Document, Cluster
from library.math_modified import exponentialDecay, DateTimeAirthematic
from library.classes import TwoWayMap

class UtilityMethods:
    @staticmethod
    def createOrAddNewPhraseObject(phrase, phraseTextToPhraseObjectMap, occuranceTime, **stream_settings):
        if phrase not in phraseTextToPhraseObjectMap: phraseTextToPhraseObjectMap[phrase] = Phrase(phrase, occuranceTime, score=1)
        else: phraseTextToPhraseObjectMap[phrase].updateScore(occuranceTime, scoreToUpdate=1, **stream_settings)
    @staticmethod
    def updatePhraseTextToPhraseObject(phraseVector, occuranceTime, phraseTextToPhraseObjectMap, **stream_settings): 
        [UtilityMethods.createOrAddNewPhraseObject(phrase, phraseTextToPhraseObjectMap, occuranceTime, **stream_settings) for phrase in phraseVector]
    @staticmethod
    def updateDimensions(phraseTextAndDimensionMap, phraseTextToPhraseObjectMap, currentTime, **stream_settings):
        '''
        Update phraseTextAndDimensionMap with new dimensions.
        '''
        print '****** Checking for our condition.'
        if len(phraseTextAndDimensionMap.getMap(TwoWayMap.MAP_FORWARD).values())!=len(set(phraseTextAndDimensionMap.getMap(TwoWayMap.MAP_FORWARD).values())): print 'Multiple phrases with same id.', exit()
        def getNextNewPhrase(topPhrasesSet):
            for phrase in topPhrasesSet: 
                if not phraseTextAndDimensionMap.contains(TwoWayMap.MAP_FORWARD, phrase): yield phrase
        def getNextAvailableId(setOfIds): 
            for id in setOfIds: yield id
        def updatePhraseScore(phraseObject): 
            phraseObject.updateScore(currentTime, 0, **stream_settings)
            return phraseObject
        UtilityMethods.pruneUnnecessaryPhrases(phraseTextToPhraseObjectMap, currentTime, UtilityMethods.pruningConditionDeterministic, **stream_settings)
        topPhrasesList = [p.text for p in Phrase.sort((updatePhraseScore(p) for p in phraseTextToPhraseObjectMap.itervalues()), reverse=True)[:stream_settings['max_dimensions']]]
#        print topPhrasesList[:10]
        topPhrasesSet = set(topPhrasesList)
        newPhraseIterator = getNextNewPhrase(topPhrasesSet)
        availableIds = set(list(range(stream_settings['max_dimensions'])))
        for phrase in phraseTextAndDimensionMap.getMap(TwoWayMap.MAP_FORWARD).keys()[:]:
            availableIds.remove(phraseTextAndDimensionMap.get(TwoWayMap.MAP_FORWARD, phrase))
            if phrase not in topPhrasesSet:
                try:
                    newPhrase = newPhraseIterator.next()
                    phraseTextAndDimensionMap.set(TwoWayMap.MAP_FORWARD, newPhrase, phraseTextAndDimensionMap.get(TwoWayMap.MAP_FORWARD, phrase))
                except StopIteration: continue
                finally: phraseTextAndDimensionMap.remove(TwoWayMap.MAP_FORWARD, phrase)
        availableIdsIterator = getNextAvailableId(availableIds)
        while True: 
            try:
                phraseTextAndDimensionMap.set(TwoWayMap.MAP_FORWARD, newPhraseIterator.next(), availableIdsIterator.next())
            except StopIteration: break
        UtilityMethods.checkCriticalErrorsInPhraseTextToIdMap(phraseTextAndDimensionMap, **stream_settings)
    @staticmethod
    def checkCriticalErrorsInPhraseTextToIdMap(phraseTextAndDimensionMap, **stream_settings):
        if len(phraseTextAndDimensionMap)>stream_settings['max_dimensions']: print 'Illegal number of dimensions.', exit()
        if len(phraseTextAndDimensionMap.getMap(TwoWayMap.MAP_FORWARD).values())!=len(set(phraseTextAndDimensionMap.getMap(TwoWayMap.MAP_FORWARD).values())): print 'Multiple phrases with same id.', exit()
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
        self.lastMessageTime = message.timeStamp

class StreamCluster(Cluster):
    def __init__(self, stream, score=1):
        super(StreamCluster, self).__init__(stream)
        self.lastStreamAddedTime, self.score = stream.lastMessageTime, score
    def addStream(self, stream, **stream_settings):
        super(StreamCluster, self).addDocument(stream)
        self.updateScore(stream.lastMessageTime, scoreToUpdate=1, **stream_settings)
    def updateScore(self, currentOccuranceTime, scoreToUpdate, **stream_settings):
        timeDifference = DateTimeAirthematic.getDifferenceInTimeUnits(currentOccuranceTime, self.lastStreamAddedTime, stream_settings['time_unit_in_seconds'].seconds)
        self.score=exponentialDecay(self.score, stream_settings['stream_cluster_decay_coefficient'], timeDifference)+scoreToUpdate
        self.lastStreamAddedTime=currentOccuranceTime
        
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

if __name__ == '__main__':
    class C:
        def __init__(self): self.attr1, self.attr2 = 0, 0
    c = C()
    print c.__dict__
    