'''
Created on Sep 14, 2011

@author: kykamath
'''
from experiments.quality_comparison_with_kmeans import TweetsFile
from library.file_io import FileIO
import matplotlib.pyplot as plt
import numpy as np
from settings import default_experts_twitter_stream_settings

def iterateData():
#    for nonOptimzed, optimized in zip(FileIO.iterateJsonFromFile(TweetsFile.default_stats_file), FileIO.iterateJsonFromFile(TweetsFile.stats_file)): yield nonOptimzed, optimized
    for nonOptimzed, optimized in zip(FileIO.iterateJsonFromFile('default_stats_file'), FileIO.iterateJsonFromFile('quality_stats')): yield nonOptimzed, optimized

#for nonOptimzed, optimized in iterateData():
#    print 'non-opt', nonOptimzed['streaming_lsh']['iteration_time'], nonOptimzed['streaming_lsh']['nmi'], nonOptimzed['streaming_lsh']['no_of_documents'], nonOptimzed['settings']['stream_id']
#    print 'opt', optimized['streaming_lsh']['iteration_time'], optimized['streaming_lsh']['nmi'], optimized['streaming_lsh']['no_of_documents'], optimized['settings']['stream_id']
    
def plotTime():
    dataX, optTime, unOptTime = [], [], []
    for nonOptimzed, optimized in iterateData():
        dataX.append(optimized['streaming_lsh']['no_of_documents'])
        optTime.append(optimized['streaming_lsh']['iteration_time'])
        unOptTime.append(nonOptimzed['streaming_lsh']['iteration_time'])
#        print 'non-opt', nonOptimzed['streaming_lsh']['iteration_time'], nonOptimzed['streaming_lsh']['nmi'], nonOptimzed['streaming_lsh']['no_of_documents'], nonOptimzed['settings']['stream_id']
#        print 'opt', optimized['streaming_lsh']['iteration_time'], optimized['streaming_lsh']['nmi'], optimized['streaming_lsh']['no_of_documents'], optimized['settings']['stream_id']
    plt.plot(dataX, optTime, label='opt')
    plt.plot(dataX, unOptTime, label='un-opt')
    plt.legend()
    plt.savefig('plt_time.eps')

def plotQuality():
    dataX, optQuality, unOptQuality = [], [], []
    for nonOptimzed, optimized in iterateData():
        dataX.append(optimized['streaming_lsh']['no_of_documents'])
        optQuality.append(optimized['streaming_lsh']['nmi'])
        unOptQuality.append(nonOptimzed['streaming_lsh']['nmi'])
#    print 'opt', np.mean(optQuality)
#    print 'un opt', np.mean(unOptQuality)
    plt.plot(dataX, optQuality, label='opt')
    plt.plot(dataX, unOptQuality, label='un-opt')
    plt.legend()
    plt.savefig('plt_quality.eps')
if __name__ == '__main__':
#    plotQuality()
#    plotTime()

    tf = TweetsFile(1000, **default_experts_twitter_stream_settings)
    for i in tf.documents:
        print i