'''
Created on Sep 30, 2011

@author: kykamath
'''
from library.twitter import TweetFiles
from library.file_io import FileIO
time_to_process_points = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/time_to_process_points/'

def generateData():
    i = 0
    for line in TweetFiles.iterateTweetsFromGzip('/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/data/1000000.gz'):
        FileIO.writeToFileAsJson(line, time_to_process_points+'%s'%(i/50000))
        i+=1
#        if i==10: break

generateData()