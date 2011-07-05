'''
Created on Jun 22, 2011

@author: kykamath
'''
from library.classes import Settings
from datetime import timedelta
from library.math_modified import getLargestPrimeLesserThan
from library.file_io import FileIO

twitterDataFolder='/mnt/chevron/kykamath/data/twitter/'

# General twitter stream settings.
twitter_stream_settings = Settings(
                                   stream_id='twitter_stream', # Unique id to represent the stream.
                                   max_dimensions=99991, # Number of maximum dimensions to consider at a time. Make sue this is prime. This is also equal to the number of top phrases that will be considered for crowd discovery.
                                   min_phrase_length=2, # Minumum lenght of phrases. For example min_phrase_length=1 and max_phrase_length=1 will result in only unigrams as features.
                                   max_phrase_length=2, # Maximum lenght of phrases. For example min_phrase_length=1 and max_phrase_length=2 will result in both unigrams and bigrams as features.
                                   
                                   phrase_decay_coefficient=0.75, # The rate at which phrases decays.
                                   stream_decay_coefficient=0.75, # The rate at which stream decays.
                                   stream_cluster_decay_coefficient=0.5, # The rate at which a cluster decays.
                                   
                                   time_unit_in_seconds=timedelta(seconds=5*60), # This value will be used to determine the length of unit time intervals.
                                   
                                   dimension_update_frequency_in_seconds=timedelta(seconds=15*60), # Every these many seconds, old phrases are pruned and new dimensions are created.
                                   max_phrase_inactivity_time_in_seconds=timedelta(seconds=30*60), # Time after which a phrase can be considered old and need not be tracked.
                                   
                                   # Cluster pruning properties.
                                   cluster_filter_attribute = 'length', # The attribute based on which stream clusters will be pruned. 'length' => Size of clusters; score => streaming cluster score.
                                   cluster_filter_threshold = 5, # Value for the cluster filter threshold. All clusters with attribute values below this will be pruned.
                                   cluster_merging_jaccard_distance_threshold = 0.4 # Clusters are merged if the jaccard threshold is above this value. 
                                   )

# Streaming LSH clustering specific settings.
streaming_lsh_settings=Settings(
                                dimensions=twitter_stream_settings.max_dimensions,
                                signature_length=23,
                                number_of_permutations=13,
                                threshold_for_document_to_be_in_cluster=0.005
                                )
twitter_stream_settings.update(streaming_lsh_settings)

# Settings for trends specific streams.
trends_twitter_stream_settings = Settings()
trends_twitter_stream_settings.update(twitter_stream_settings)
trends_twitter_stream_settings.stream_id = 'trends_twitter_stream'

# Settings for expert specific streams.
experts_twitter_stream_settings = Settings()
experts_twitter_stream_settings.update(twitter_stream_settings)
experts_twitter_stream_settings.stream_id = 'experts_twitter_stream'
experts_twitter_stream_settings.plot_color = '#F28500'
experts_twitter_stream_settings.plot_label = 'Experts stream'
experts_twitter_stream_settings.dimension_update_frequency_in_seconds=timedelta(seconds=30*60)
experts_twitter_stream_settings.cluster_filter_threshold = 2
experts_twitter_stream_settings.twitter_users_tweets_folder='%susers/tweets/'%twitterDataFolder
experts_twitter_stream_settings.users_to_crawl_file='%susers/crawl/users_to_crawl'%twitterDataFolder
experts_twitter_stream_settings.lsh_clusters_folder='%slsh_crowds/experts_stream/clusters/'%twitterDataFolder
experts_twitter_stream_settings.parameter_estimation_folder='%slsh_crowds/experts_stream/parameter_estimation/'%twitterDataFolder

# Settings for houston specific streams.
houston_twitter_stream_settings = Settings()
houston_twitter_stream_settings.update(twitter_stream_settings)
houston_twitter_stream_settings.stream_id = 'houston_twitter_stream'
houston_twitter_stream_settings.plot_color = '#CC00FF'
houston_twitter_stream_settings.plot_label = 'Houston stream'
houston_twitter_stream_settings.twitter_users_tweets_folder='%shouston/'%twitterDataFolder
houston_twitter_stream_settings.lsh_clusters_folder='%slsh_crowds/houston_stream/clusters/'%twitterDataFolder
houston_twitter_stream_settings.parameter_estimation_folder='%slsh_crowds/houston_stream/parameter_estimation/'%twitterDataFolder

if __name__ == '__main__':
    print getLargestPrimeLesserThan(23)