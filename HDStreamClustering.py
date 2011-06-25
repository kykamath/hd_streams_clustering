'''
Created on Jun 25, 2011

@author: kykamath
'''
from streaming_lsh.StreamingLSHClustering import StreamingLSHClustering

class HDStreaminClustering(StreamingLSHClustering):
    def __init__(self, **stream_settings):
        super(HDStreaminClustering, self).__init__(**stream_settings)
        