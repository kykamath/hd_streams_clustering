'''
Created on Jun 22, 2011

@author: kykamath
'''
import random
from streaming_lsh.classes import Document, Cluster
from library.math_modified import exponentialDecay, DateTimeAirthematic
from library.classes import TwoWayMap, GeneralMethods, timeit
from library.vector import Vector
from library.clustering import EvaluationMetrics
from datetime import datetime

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
        def getNextNewPhrase(topPhrasesSet):
            for phrase in topPhrasesSet: 
                if not phraseTextAndDimensionMap.contains(TwoWayMap.MAP_FORWARD, phrase): yield phrase
        def getNextAvailableId(setOfIds): 
            for id in setOfIds: yield id
        def updatePhraseScore(phraseObject): 
            phraseObject.updateScore(currentTime, 0, **stream_settings)
            return phraseObject
        UtilityMethods.pruneUnnecessaryPhrases(phraseTextToPhraseObjectMap, currentTime, UtilityMethods.pruningConditionDeterministic, **stream_settings)
        topPhrasesList = [p.text for p in Phrase.sort((updatePhraseScore(p) for p in phraseTextToPhraseObjectMap.itervalues()), reverse=True)[:stream_settings['dimensions']]]
        newPhraseIterator = getNextNewPhrase(topPhrasesList)
        availableIds = set(list(range(stream_settings['dimensions'])))
#        @timeit
#        def meth1():
#            for phrase in phraseTextAndDimensionMap.getMap(TwoWayMap.MAP_FORWARD).keys()[:]:
#                availableIds.remove(phraseTextAndDimensionMap.get(TwoWayMap.MAP_FORWARD, phrase))
#                if phrase not in topPhrasesList:
#                    try:
#                        newPhrase = newPhraseIterator.next()
#                        phraseTextAndDimensionMap.set(TwoWayMap.MAP_FORWARD, newPhrase, phraseTextAndDimensionMap.get(TwoWayMap.MAP_FORWARD, phrase))
#                    except StopIteration: continue
#                    finally: phraseTextAndDimensionMap.remove(TwoWayMap.MAP_FORWARD, phrase)
        phrasesNotInTopPhrasesList = []
        topPhrasesSet = set(topPhrasesList)
        def modifiedMeth1():
            @timeit
            def m1():
                for phrase in phraseTextAndDimensionMap.getMap(TwoWayMap.MAP_FORWARD).keys():
                    availableIds.remove(phraseTextAndDimensionMap.get(TwoWayMap.MAP_FORWARD, phrase))
            @timeit
            def m2():
                for phrase in phraseTextAndDimensionMap.getMap(TwoWayMap.MAP_FORWARD).keys():
                    if phrase not in topPhrasesSet: phrasesNotInTopPhrasesList.append(phrase)
            m1(), m2()
        
        print len(phraseTextAndDimensionMap), len(topPhrasesSet)
        modifiedMeth1()
        print len(phraseTextAndDimensionMap), len(topPhrasesList)
        for phrase in phrasesNotInTopPhrasesList:
            try:
                newPhrase = newPhraseIterator.next()
                phraseTextAndDimensionMap.set(TwoWayMap.MAP_FORWARD, newPhrase, phraseTextAndDimensionMap.get(TwoWayMap.MAP_FORWARD, phrase))
            except StopIteration: pass
            phraseTextAndDimensionMap.remove(TwoWayMap.MAP_FORWARD, phrase)
        availableIdsIterator = getNextAvailableId(availableIds)
        while True: 
            try:
                p = newPhraseIterator.next()
                i = availableIdsIterator.next()
                phraseTextAndDimensionMap.set(TwoWayMap.MAP_FORWARD, p, i)
            except StopIteration: break
        UtilityMethods.checkCriticalErrorsInPhraseTextToIdMap(phraseTextAndDimensionMap, **stream_settings)
    @staticmethod
    def checkCriticalErrorsInPhraseTextToIdMap(phraseTextAndDimensionMap, **stream_settings):
        if len(phraseTextAndDimensionMap)>stream_settings['dimensions']: print 'Illegal number of dimensions.', exit()
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
    def __init__(self, stream, **kwargs):
        super(StreamCluster, self).__init__(stream, **kwargs)
        self.lastStreamAddedTime = stream.lastMessageTime
    def addDocument(self, stream, **stream_settings):
        super(StreamCluster, self).addDocument(stream)
        # Adding this next line to analyze cluster lag distributions. Remove this code if experiments are not needed.
        if 'lag_between_streams' in stream_settings: stream_settings['lag_between_streams'](self, stream, **stream_settings)
        self.lastStreamAddedTime = stream.lastMessageTime
#        self.updateScore(stream.lastMessageTime, scoreToUpdate=1, **stream_settings)
#    def updateScore(self, currentOccuranceTime, scoreToUpdate, **stream_settings):
#        timeDifference = DateTimeAirthematic.getDifferenceInTimeUnits(currentOccuranceTime, self.lastStreamAddedTime, stream_settings['time_unit_in_seconds'].seconds)
#        self.score=exponentialDecay(self.score, stream_settings['stream_cluster_decay_coefficient'], timeDifference)+scoreToUpdate
#        self.lastStreamAddedTime=currentOccuranceTime
    def mergeCluster(self, otherStreamCluster):
        StreamCluster.updateClusterAttributes(self, otherStreamCluster)
        if self.lastStreamAddedTime < otherStreamCluster.lastStreamAddedTime: self.lastStreamAddedTime=otherStreamCluster.lastStreamAddedTime
    @staticmethod
    def getClusterObjectToMergeFrom(streamCluster):
        streamCluster.lastMessageTime = streamCluster.lastStreamAddedTime
        mergedCluster = StreamCluster(streamCluster, shouldUpdateDocumentId=False)
        mergedCluster.aggregateVector, mergedCluster.vectorWeights  = Vector({}), 0.0
        StreamCluster.updateClusterAttributes(mergedCluster, streamCluster)
        return mergedCluster
    @staticmethod
    def updateClusterAttributes(newStreamCluster, oldStreamCluster):
        newStreamCluster.aggregateVector+=oldStreamCluster.aggregateVector
        newStreamCluster.vectorWeights+=oldStreamCluster.vectorWeights
        [newStreamCluster.updateDocumentId(document) for document in oldStreamCluster.iterateDocumentsInCluster()]
        for k in newStreamCluster.aggregateVector: newStreamCluster[k]=newStreamCluster.aggregateVector[k]/newStreamCluster.vectorWeights

class Crowd:
    '''
    Clusters combined over time.
    '''
    def __init__(self, cluster, clusterFormationTime):
        self.crowdId = cluster.clusterId
        self.clusters = {GeneralMethods.getEpochFromDateTimeObject(clusterFormationTime): cluster}
        self.ends, self.inComingCrowds, self.outGoingCrowd = False, [], None
    @property
    def lifespan(self): return len(self.clusters)
    @property
    def topDimensions(self, numberOfDimensions=10):return Vector.getMeanVector(self.clusters.itervalues()).getTopDimensions(numberOfFeatures=numberOfDimensions)
    @property
    def hashtagDimensions(self): return reduce(lambda x,y: x.union(Crowd.getHashtags(y)), self.clusters.itervalues(), set())
    @property
    def startTime(self): return datetime.fromtimestamp(sorted(self.clusters)[0])
    @property
    def endTime(self): return datetime.fromtimestamp(sorted(self.clusters)[-1])
    def append(self, cluster, clusterFormationTime): self.clusters[GeneralMethods.getEpochFromDateTimeObject(clusterFormationTime)]=cluster
    def updateOutGoingCrowd(self, crowdId): self.ends, self.outGoingCrowd = True, crowdId
    def updateInComingCrowd(self, crowdId): self.inComingCrowds.append(crowdId)
    def getCrowdQuality(self, evaluationMetric, expertsToClassMap):
        def getExpertClasses(cluster): return [expertsToClassMap[user.lower()] for user in cluster.documentsInCluster if user.lower() in expertsToClassMap]
        return EvaluationMetrics.getValueForClusters([getExpertClasses(cluster) for cluster in self.clusters.itervalues()], evaluationMetric)
    @staticmethod
    def getHashtags(cluster): return set([w for d in cluster for w in d.split() if w.startswith('#')])

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
    