'''
Created on Jul 15, 2011

@author: kykamath
'''
from library.file_io import FileIO
from library.mr_algorithms.kmeans import KMeans
import numpy as np

clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'
clustering_quality_experts_mr_folder = clustering_quality_experts_folder+'mr_data/'

def extractArraysFromFile(file, numberOfArrays=float('+inf')):
        arraysToReturn = []
        for line in FileIO.iterateJsonFromFile(file):
            if numberOfArrays==0: break;
            else: numberOfArrays-=1
            arraysToReturn.append(np.array(line['vector']))
        return arraysToReturn
fileName = clustering_quality_experts_mr_folder+'100'
#    print fileName
print list(KMeans.cluster(fileName, extractArraysFromFile(fileName), mrArgs='-r hadoop', iterations=5))