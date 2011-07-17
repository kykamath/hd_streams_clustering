'''
Created on Jul 16, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
from library.vector import Vector
from collections import defaultdict

ssa_threshold=0.75

class SSASimilarityMR(ModifiedMRJob):
    def __init__(self,*args, **kwargs):
        super(SSASimilarityMR, self).__init__(*args, **kwargs)
        self.ssa_threshold = ssa_threshold
        self.streamIdToSimilarStreamsMap = defaultdict(set)
        self.observedStreams, self.hasSeenAnOlderStream = set(), False
    def mapper(self, _, value):
        if False: yield # I'm a generator!
        [(id0, vec0), (id1, vec1)] = value
        vec0, vec1 = Vector(vec0), Vector(vec1)
        if vec0.cosineSimilarity(vec1)>=self.ssa_threshold: self.streamIdToSimilarStreamsMap[id0].add(id1) if id0<id1 else self.streamIdToSimilarStreamsMap[id1].add(id0)
    def mapper_final(self):
        for id, ssids in self.streamIdToSimilarStreamsMap.iteritems():
            for ssid in ssids: 
                yield id, ssid
    def reducer(self, key, values):
        possibleClusterIds, otherStreams = [key], []
        for v in values:
#            if type(v)==type({}): possibleClusterIds.append(v['id'])
#            else: 
            if not self.hasSeenAnOlderStream: 
                if v in self.observedStreams: 
                    self.hasSeenAnOlderStream = True
                    yield 'stream_in_multiple_clusters', []
                else: self.observedStreams.add(v)
            otherStreams.append(v)
#        clusterId = min(possibleClusterIds)
        yield key, {'cid': None, 's':otherStreams}
    def steps(self): return [self.mr(mapper=self.mapper, reducer=self.reducer, mapper_final=self.mapper_final)]

if __name__ == '__main__':
    SSASimilarityMR.run()