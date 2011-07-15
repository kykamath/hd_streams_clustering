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
hdfsPath='hdfs:///user/kykamath/lsh_experts_data/'

def extractArraysFromFile(file, percentage=1.0):
        arraysToReturn = []
        for line in FileIO.iterateJsonFromFile(file): arraysToReturn.append(np.array(line['vector']))
        return arraysToReturn[:int(len(arraysToReturn)*percentage)]

length=10000
localFileName = clustering_quality_experts_mr_folder+'%s'%length
hdfsFileName = hdfsPath+'%s'%length
clusters = list(KMeans.cluster(hdfsFileName, extractArraysFromFile(localFileName,0.9), mrArgs='-r hadoop', iterations=1, jobconf={'mapred.map.tasks':100}))
print clusters
distribution = defaultdict(int)
for clusterId, cluster in clusters:
    distribution[len(cluster)]+=1
for k in sorted(distribution):
    print k, distribution[k]
    
#iterationData =  {'no_of_documents':self.length, 'no_of_clusters': len(documentClusters), 'iteration_time': te-ts, 'clusters': documentClusters}
#self.getEvaluationMetrics([self._getExpertClasses(cluster) for cluster in documentClusters], iterationData)