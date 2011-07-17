'''
Created on Jul 16, 2011

@author: kykamath
'''
from library.mrjobwrapper import ModifiedMRJob
#from mrjob.job import MRJob
#from mrjob.protocol import DEFAULT_PROTOCOL

class SimilarStreamAggregationMR(ModifiedMRJob):
    def mapper(self, key, value):
        self.set_status(value.keys())
        yield key, value

if __name__ == '__main__':
    SimilarStreamAggregationMR.run()