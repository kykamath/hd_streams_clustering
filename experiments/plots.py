'''
Created on Sep 14, 2011

@author: kykamath
'''
from experiments.quality_comparison_with_kmeans import TweetsFile
from library.file_io import FileIO
from settings import experts_twitter_stream_settings, default_experts_twitter_stream_settings

#for data in FileIO.iterateJsonFromFile(TweetsFile.combined_stats_file):
#    print data['streaming_lsh']['iteration_time'], data['streaming_lsh']['no_of_documents'], data['settings']['stream_id']
#for data in FileIO.iterateJsonFromFile(TweetsFile.default_stats_file):
#    print data['streaming_lsh']['iteration_time'], data['streaming_lsh']['no_of_documents'], data['settings']['stream_id']
    
for nonOptimzed, optimized in zip(FileIO.iterateJsonFromFile(TweetsFile.default_stats_file), FileIO.iterateJsonFromFile(TweetsFile.combined_stats_file)):
    print 'non-opt', nonOptimzed['streaming_lsh']['iteration_time'], nonOptimzed['streaming_lsh']['nmi'], nonOptimzed['streaming_lsh']['no_of_documents'], nonOptimzed['settings']['stream_id']
    print 'opt', optimized['streaming_lsh']['iteration_time'], optimized['streaming_lsh']['nmi'], optimized['streaming_lsh']['no_of_documents'], optimized['settings']['stream_id']
