'''
Created on Jul 12, 2011

@author: kykamath
'''
from twitter_streams_clustering import TwitterIterators
import os
from library.file_io import FileIO

clustering_quality_experts_data_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_data_folder/'

class GenerateData:
    @staticmethod
    def forLength(length):
        fileName=clustering_quality_experts_data_folder+str(length)
        for tweet in TwitterIterators.iterateTweetsFromExperts(): FileIO.writeToFileAsJson(tweet, fileName) 
        os.system('gzip %s'%fileName)

if __name__ == '__main__':
    [GenerateData.forLength(i*j) for i in [10**3, 10**4, 10**5] for j in range(1, 10)]

