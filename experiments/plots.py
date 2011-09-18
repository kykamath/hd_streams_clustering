'''
Created on Sep 14, 2011

@author: kykamath
'''
import sys, os, time
from library.plotting import getLatexForString
sys.path.append('../')
from classes import Stream
from library.file_io import FileIO
import matplotlib.pyplot as plt
from hd_streams_clustering import HDStreaminClustering
import numpy as np
from settings import default_experts_twitter_stream_settings, experts_twitter_stream_settings
from library.clustering import EvaluationMetrics
from twitter_streams_clustering import TwitterIterators, getExperts,\
    TwitterCrowdsSpecificMethods
from library.nlp import getPhrases, getWordsFromRawEnglishMessage
from algorithms_performance import emptyClusterAnalysisMethod, emptyClusterFilteringMethod
from settings import Settings

OPTIMIZED_ID = 'optimized'
UN_OPTIMIZED_ID = 'un_optimized'

clustering_quality_hd_experiments_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_hd_experiments_folder/'
clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'
clustering_quality_experts_ssa_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_ssa_folder/'
hd_clustering_performance_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/hd_clustering_performance/'

experts_twitter_stream_settings['status_file'] = clustering_quality_hd_experiments_folder+'optimized_stats_file'
default_experts_twitter_stream_settings['status_file'] = clustering_quality_hd_experiments_folder+'unoptomized_stats_file'


algorithm_info = {
                  'cda': {'label': 'opt', 'color': 'k'},
                  'cda_unopt': {'label': 'Streaming-CDA', 'color': '#7109AA'},
                  'cda_it': {'label': 'cda_it', 'color': 'm'},
                  'cda_mr': {'label': 'cda_mr', 'color': 'y'},
                  'kmeans': {'label': 'k-Means', 'color': '#FD0006'},
                  'kmeans_mr': {'label': 'MR k-Means', 'color': '#5AF522'}
                  }

class DataIterators:
    @staticmethod
    def kmeans(): 
        for data in FileIO.iterateJsonFromFile(clustering_quality_experts_folder+'quality_stats'): yield data['k_means']
    @staticmethod
    def kmeansmr(): 
        for data in FileIO.iterateJsonFromFile(clustering_quality_experts_folder+'mr_quality_stats'): yield data['mr_k_means']
    @staticmethod
    def cdait(): 
        for data in FileIO.iterateJsonFromFile(clustering_quality_experts_ssa_folder+'quality_stats'): yield data['ssa']
    @staticmethod
    def cdamr(): 
        for data in FileIO.iterateJsonFromFile(clustering_quality_experts_ssa_folder+'quality_stats'): yield data['ssa_mr']
    @staticmethod
    def optimized(): 
        for data in FileIO.iterateJsonFromFile(hd_clustering_performance_folder+'cda'): yield data['streaming_lsh']
    @staticmethod
    def unoptimized(): 
        for data in FileIO.iterateJsonFromFile(hd_clustering_performance_folder+'cda_unopt'): yield data['streaming_lsh']

class CompareAlgorithms:
    @staticmethod
    def runningTimes(*iterators, **kwargs):
        loc = kwargs.get('loc', 1)
        fileName = kwargs.get('file_name', 'running_times.eps')
        xmax = kwargs.get('xmax', None)
        xmin = kwargs.get('xmin', None)
        title = kwargs.get('title', None)
        for id, iterator in iterators:
            dataX, dataY = [], []
            for data in iterator:
                dataX.append(data['no_of_documents']), dataY.append(data['iteration_time'])
            plt.loglog(dataX, dataY, label=algorithm_info[id]['label'], color=algorithm_info[id]['color'], lw=2)
        plt.legend(loc=loc)
        plt.xlabel(getLatexForString('\# of documents')); plt.ylabel(getLatexForString('Running time (s)')); plt.title(getLatexForString(title))
        if xmax: plt.xlim(xmax=xmax) 
        if xmin: plt.xlim(xmin=xmin) 
        plt.savefig(fileName)
        
    @staticmethod
    def quality(quality_type, *iterators):
        for id, iterator in iterators:
            dataY = []
            for data in iterator: dataY.append(data[quality_type])
            print id, np.mean(dataY)

if __name__ == '__main__':
#    plotQuality()
#    plotTime()
#    TweetsFile.generateStatsFor(experts_twitter_stream_settings)
#    TweetsFile.generateStatsFor(default_experts_twitter_stream_settings)

    CompareAlgorithms.runningTimes(
                                   ('kmeans', DataIterators.kmeans()), 
                                   ('kmeans_mr', DataIterators.kmeansmr()), 
                                   ('cda_unopt', DataIterators.unoptimized()),
                                   loc=2, file_name='running_time_kmeans.eps', xmin=800, xmax=95000,
                                   title='Running time comparison of Streaming-CDA with k-Means'
                                )
#    CompareAlgorithms.runningTimes(('cda_it', DataIterators.cdait()), ('cda', DataIterators.optimized()), ('cda_mr', DataIterators.cdamr()), ('cda_unopt', DataIterators.unoptimized()))
#    CompareAlgorithms.runningTimes(('cda', DataIterators.optimized()), ('cda_unopt', DataIterators.unoptimized()))

#    CompareAlgorithms.quality('purity', ('cda', DataIterators.optimized()), ('cda_unopt', DataIterators.unoptimized()))
#    CompareAlgorithms.quality('nmi', ('cda', DataIterators.optimized()), ('cda_unopt', DataIterators.unoptimized()))
#    for d in DataIterators.cdamr(): 
#        del d['clusters']
#        print d
