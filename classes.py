'''
Created on Jun 22, 2011

@author: kykamath
'''

from streaming_lsh.classes import Document
from library.nlp import getPhrases, getWordsFromRawEnglishMessage
from collections import defaultdict
from library.vector import Vector

class VectorUpdateMethods:
    @staticmethod
    def addWithoutDecay(stream, vector): 
        for k,v in vector.iteritems():
            if k in stream: stream[k]+=v
            else: stream[k]=v
        
class Stream(Document):
    def __init__(self, streamId, vector): super(Stream, self).__init__(streamId, vector)
    def updateWithVector(self, vector, updateMethod): 
        self=updateMethod(self,vector)


class Message:
    def __init__(self, streamId, messageId, text, timestamp): 
        self.streamId, self.messageId, self.text, self.timestamp = streamId, messageId, text, timestamp
    def getVector(self, wordToIdMap, min_phrase_length, max_phrase_length):
        vectorMap = defaultdict(float)
        for phrase in getPhrases(getWordsFromRawEnglishMessage(self.text), min_phrase_length, max_phrase_length): vectorMap[wordToIdMap[phrase]]+=1
        self.vector = Vector(vectorMap)
        return self.vector