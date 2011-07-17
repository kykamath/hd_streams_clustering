'''
Created on Jul 17, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
from collections import defaultdict

class SSAAggrigationMR(ModifiedMRJob):
    def __init__(self,*args, **kwargs):
        super(SSAAggrigationMR, self).__init__(*args, **kwargs)
        self.streamIdToSimilarStreamsMap = defaultdict(list)
        self.observedStreams, self.hasSeenAnOlderStream = set(), False
    def mapper(self, id, value):
        if False: yield # I'm a generator!
        clusterId = id if value['cid']==None else min(id, value['cid'])
        for s in value['s']:
            if id!=clusterId or value['cid']==None: 
                self.streamIdToSimilarStreamsMap[s].append({'cid': clusterId})
            self.streamIdToSimilarStreamsMap[clusterId].append(s)
    def mapper_final(self):
        for id, ssids in self.streamIdToSimilarStreamsMap.iteritems():
            for item in ssids: yield id, item
    def reducer(self, key, values):
        possibleClusterIds, otherStreams = [key], set()
        for v in values:
            if type(v)==type({}): possibleClusterIds.append(v['cid'])
            else: 
                if not self.hasSeenAnOlderStream: 
                    if v in self.observedStreams: 
                        self.hasSeenAnOlderStream = True
                        yield 'stream_in_multiple_clusters', []
                    else: self.observedStreams.add(v)
                otherStreams.add(v)
        clusterId = min(possibleClusterIds)
        yield key, {'cid': clusterId, 's':list(otherStreams)}

if __name__ == '__main__':
    SSAAggrigationMR.run()