'''
Created on Sep 14, 2011

@author: kykamath
'''
from library.file_io import FileIO
from library.plotting import getLatexForString
from settings import default_experts_twitter_stream_settings, \
    experts_twitter_stream_settings
import matplotlib.pyplot as plt
import numpy as np
import sys
sys.path.append('../')

OPTIMIZED_ID = 'optimized'
UN_OPTIMIZED_ID = 'un_optimized'

clustering_quality_hd_experiments_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_hd_experiments_folder/'
clustering_quality_experts_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_folder/'
clustering_quality_experts_ssa_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_ssa_folder/'
hd_clustering_performance_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/hd_clustering_performance/'

experts_twitter_stream_settings['status_file'] = clustering_quality_hd_experiments_folder+'optimized_stats_file'
default_experts_twitter_stream_settings['status_file'] = clustering_quality_hd_experiments_folder+'unoptomized_stats_file'


algorithm_info = {
                  'cda': {'label': 'Tailored Streaming-CDA', 'color': '#56F2F5', 'marker': 's'},
                  'cda_unopt': {'label': 'Streaming-CDA', 'color': '#7109AA', 'marker': '*'},
                  'cda_it': {'label': 'Iterative CDA', 'color': '#FD0006', 'marker': 'o'},
                  'cda_mr': {'label': 'MR CDA', 'color': '#5AF522', 'marker': '>'},
                  'kmeans': {'label': 'k-Means', 'color': '#FD0006', 'marker': 'o'},
                  'kmeans_mr': {'label': 'MR k-Means', 'color': '#5AF522', 'marker': '>'}
                  }

class DataIterators:
    @staticmethod
    def kmeans(): 
        for data in FileIO.iterateJsonFromFile(clustering_quality_experts_folder+'combined_stats_file'): yield data['k_means']
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
        semilog = kwargs.get('log', False)
        xmin = kwargs.get('xmin', None)
        title = kwargs.get('title', None)
        for id, iterator in iterators:
            dataX, dataY = [], []
            for data in iterator:
                if xmax and data['no_of_documents'] <= xmax: dataX.append(data['no_of_documents']), dataY.append(data['iteration_time'])
                else: dataX.append(data['no_of_documents']), dataY.append(data['iteration_time'])
            if not semilog: plt.loglog(dataX, dataY, label=algorithm_info[id]['label'], color=algorithm_info[id]['color'], lw=2, marker=algorithm_info[id]['marker'])
            else: plt.semilogx(dataX, dataY, label=algorithm_info[id]['label'], color=algorithm_info[id]['color'], lw=2, marker=algorithm_info[id]['marker'])
        plt.legend(loc=loc)
        plt.xlabel(getLatexForString('\# of documents')); plt.ylabel(getLatexForString('Running time (s)')); plt.title(getLatexForString(title))
        if xmax: plt.xlim(xmax=xmax) 
        if xmin: plt.xlim(xmin=xmin) 
        plt.savefig(fileName)
        
    @staticmethod
    def runningTimesWithCDA(*iterators, **kwargs):
        loc = kwargs.get('loc', 1)
        fileName = kwargs.get('file_name', 'running_times.eps')
        xmax = kwargs.get('xmax', None)
        semilog = kwargs.get('log', False)
        xmin = kwargs.get('xmin', None)
        title = kwargs.get('title', None)
        dataX, dataYValues = [], []
        for id, iterator in iterators:
            dataX, dataY = [], []
            for data in iterator:
                if xmax and data['no_of_documents'] <= xmax: dataX.append(data['no_of_documents']), dataY.append(data['iteration_time'])
                else: dataX.append(data['no_of_documents']), dataY.append(data['iteration_time'])
            if not semilog: plt.loglog(dataX, dataY, label=algorithm_info[id]['label'], color=algorithm_info[id]['color'], lw=2, marker=algorithm_info[id]['marker'])
            else: plt.plot(dataX, dataY, label=algorithm_info[id]['label'], color=algorithm_info[id]['color'], lw=2, marker=algorithm_info[id]['marker'])
            dataYValues.append(dataY)
        
        dataDifference = []
        for i in range(len(dataX)): dataDifference.append(dataYValues[1][i] - dataYValues[0][i])
        plt.plot(dataX, dataDifference, label='Difference', lw=2)
        
        plt.legend(loc=loc)
        plt.xlabel(getLatexForString('\# of documents (10^4)')); plt.ylabel(getLatexForString('Running time (s)')); plt.title(getLatexForString(title))
        locs,labels = plt.xticks()
        plt.xticks(locs, map(lambda x: "%d"%(x/10000), locs))
        if xmax: plt.xlim(xmax=xmax) 
        if xmin: plt.xlim(xmin=xmin) 
        plt.savefig(fileName)
#        plt.show()
        
    @staticmethod
    def quality(quality_type, *iterators, **kwargs):
        if quality_type=='f1': print kwargs.get('f1_id')
        else: print quality_type
        for id, iterator in iterators:
            dataY = []
            for data in iterator: 
                if quality_type=='f1':
                    f1_type = None
                    if kwargs.get('f1_id')=='f1': f1_type=0
                    elif kwargs.get('f1_id')=='precision': f1_type=1
                    elif kwargs.get('f1_id')=='recall': f1_type=2
                    if type(data[quality_type])==type([]): dataY.append(data[quality_type][f1_type])
                    else: dataY.append(0)
                else: dataY.append(data[quality_type])
            print id, '%0.2f'%np.mean(dataY[2:])
            
    @staticmethod
    def plotQuality():
#        kmeans = (0.79, 0.78, 0.80, 0.80, 0.79)
#        cda_it = (0.98, 0.93, 0.81, 0.84, 0.79)
#        cda_unopt = (0.95, 0.85, 0.86, 0.84, 0.87)
#        cda = (0.96, 0.88, 0.86, 0.85, 0.88)
        kmeans = (0.79, 0.78)
        cda_it = (0.98, 0.93)
        cda_unopt = (0.95, 0.85)
        cda = (0.96, 0.88)
        
        N = len(kmeans)
        ind = np.arange(N)  # the x locations for the groups
        width = 0.1       # the width of the bars
        
        fig = plt.figure()
        ax = fig.add_subplot(111)
#        rectsKmeans = ax.bar(ind, kmeans, width, color='#FF7A7A', label='k-Means', hatch='\\')
#        rectsCdaIt = ax.bar(ind+width, cda_it, width, color='#FF7AEB', label='Iterative CDA', hatch='/')
#        rectsCdaUnopt = ax.bar(ind+2*width, cda_unopt, width, color='#7A7AFF', label='Streaming-CDA', hatch='-')
#        rectsCda = ax.bar(ind+3*width, cda, width, color='#B0B0B0', label='Tailored Streaming-CDA', hatch='x')
        rectsKmeans = ax.bar(ind, kmeans, width, color='#DCDCDC', label='k-Means', hatch='\\')
        rectsCdaIt = ax.bar(ind+width, cda_it, width, color='#808080', label='Iterative CDA', hatch='/')
        rectsCdaUnopt = ax.bar(ind+2*width, cda_unopt, width, color='#778899', label='Streaming-CDA', hatch='-')
        rectsCda = ax.bar(ind+3*width, cda, width, color='#2F4F4F', label='Tailored Streaming-CDA', hatch='x')
        
        ax.set_ylabel('Score')
        ax.set_title('Quality of crowds discovered')
        ax.set_xticks(ind+width)
#        ax.set_xticklabels( ('Purity', 'NMI', 'F1', 'Precision', 'Recall') )
        ax.set_xticklabels(('Purity', 'NMI'))
        
        plt.legend(loc=8, ncol=2)
        plt.savefig('crowds_quality.pdf')
        
if __name__ == '__main__':
#    CompareAlgorithms.runningTimes(
#                                   ('kmeans', DataIterators.kmeans()), 
#                                   ('kmeans_mr', DataIterators.kmeansmr()), 
#                                   ('cda_unopt', DataIterators.unoptimized()),
#                                   loc=2, file_name='running_time_kmeans.pdf', xmin=800, xmax=97000,
#                                   title='Running time comparison of Streaming-CDA with k-Means'
#                                )
    
#    CompareAlgorithms.runningTimes(
#                                   ('cda_it', DataIterators.cdait()), 
#                                   ('cda_mr', DataIterators.cdamr()), 
#                                   ('cda_unopt', DataIterators.unoptimized()),
#                                   loc=2, file_name='running_time_cda.pdf', xmin=800, xmax=550000,
#                                   title='Running time comparison of Streaming-CDA with other CDA'
#                                )

#    CompareAlgorithms.runningTimesWithCDA(
#                                   ('cda', DataIterators.optimized()), 
#                                   ('cda_unopt', DataIterators.unoptimized()),
#                                   loc=2, file_name='running_time_opt_unopt_cda.pdf', xmin=55000, xmax=10**6, log=True,
#                                   title='Running time performance after parameters estimation.'
#                                )
    
#    CompareAlgorithms.quality('purity', ('kmeans', DataIterators.kmeans()), ('cda_it', DataIterators.cdait()), ('cda_unopt', DataIterators.unoptimized()), ('cda', DataIterators.optimized()))
#    CompareAlgorithms.quality('nmi', ('kmeans', DataIterators.kmeans()), ('cda_it', DataIterators.cdait()), ('cda_unopt', DataIterators.unoptimized()), ('cda', DataIterators.optimized()))
#    CompareAlgorithms.quality('f1', ('kmeans', DataIterators.kmeans()), ('cda_it', DataIterators.cdait()), ('cda_unopt', DataIterators.unoptimized()), ('cda', DataIterators.optimized()), f1_id='f1')
#    CompareAlgorithms.quality('f1', ('kmeans', DataIterators.kmeans()), ('cda_it', DataIterators.cdait()), ('cda_unopt', DataIterators.unoptimized()), ('cda', DataIterators.optimized()), f1_id='precision')
#    CompareAlgorithms.quality('f1', ('kmeans', DataIterators.kmeans()), ('cda_it', DataIterators.cdait()), ('cda_unopt', DataIterators.unoptimized()), ('cda', DataIterators.optimized()), f1_id='recall')
    
    CompareAlgorithms.plotQuality()

