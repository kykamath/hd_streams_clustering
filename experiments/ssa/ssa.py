'''
Created on Jul 16, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
from library.vector import Vector
from collections import defaultdict

ssa_threshold=0.75

class Stream:
    def __init__(self, id=None): self.id=id

class SimilarStreamAggregationMR(ModifiedMRJob):
#    DEFAULT_PROTOCOL = 'json'
    def __init__(self, ssa_threshold=ssa_threshold, *args, **kwargs):
        super(SimilarStreamAggregationMR, self).__init__(*args, **kwargs)
        self.ssa_threshold = ssa_threshold
        self.streamIdToSimilarStreamsMap = defaultdict(set)
    def vector_similiarity_mapper(self, _, value):
        if False: yield # I'm a generator!
        [(id0, vec0), (id1, vec1)] = value
        vec0, vec1 = Vector(vec0), Vector(vec1)
        if vec0.cosineSimilarity(vec1)>=self.ssa_threshold: self.streamIdToSimilarStreamsMap[id0].add(id1) if id0<id1 else self.streamIdToSimilarStreamsMap[id1].add(id0)
    def vector_similiarity_mapper_final(self):
        for id, ssids in self.streamIdToSimilarStreamsMap.iteritems():
            for ssid in ssids: yield '1', ssid
        
    def steps(self):
        return [
                self.mr(mapper=self.vector_similiarity_mapper, mapper_final=self.vector_similiarity_mapper_final)
                ]

if __name__ == '__main__':
    SimilarStreamAggregationMR.run()