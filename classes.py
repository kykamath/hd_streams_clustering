'''
Created on Jun 22, 2011

@author: kykamath
'''

from streaming_lsh.classes import Document

class Stream(Document):
    def __init__(self, streamId, vector):
        super(Stream, self).__init__(streamId, vector)