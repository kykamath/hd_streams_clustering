'''
Created on Jul 15, 2011

@author: kykamath
'''
from library.file_io import FileIO
from library.mr_algorithms.kmeans import KMeans
import numpy as np
from collections import defaultdict

clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'
clustering_quality_experts_mr_folder = clustering_quality_experts_folder+'mr_data/'

def extractArraysFromFile(file, percentage=1.0):
        arraysToReturn = []
        for line in FileIO.iterateJsonFromFile(file): arraysToReturn.append(np.array(line['vector']))
        return arraysToReturn[:int(len(arraysToReturn)*percentage)]

fileName = clustering_quality_experts_mr_folder+'1000'
clusters = list(KMeans.cluster(fileName, extractArraysFromFile(fileName,0.9), mrArgs='-r hadoop', iterations=5))
print clusters
distribution = defaultdict(int)
for clusterId, cluster in clusters:
    distribution[len(cluster)]+=1
for k in sorted(distribution):
    print k, distribution[k]