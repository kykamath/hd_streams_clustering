'''
Created on Jun 22, 2011

@author: kykamath
'''
from library.classes import Settings
from datetime import timedelta

twitter_stream_settings = Settings(
                                   max_dimensions=100000, # Number of maximum dimensions to consider at a time. This is also equal to the number of top phrases that will be considered for crowd discovery.
                                   min_phrase_length=1, # Minumum lenght of phrases. For example min_phrase_length=1 and max_phrase_length=1 will result in only unigrams as features.
                                   max_phrase_length=3, # Mzximum lenght of phrases. For example min_phrase_length=1 and max_phrase_length=2 will result in both unigrams and bigrams as features.
                                   
                                   phrase_decay_coefficient=0.5, # The rate at which phrases decays.
                                   stream_decay_coefficient=0.5, # The rate at which stream decays
                                   
                                   time_unit_in_seconds=timedelta(seconds=15*60), # This value will be used to determine the length of unit time intervals. At the start of every interval the phrases are reset.
                                   max_phrase_inactivity_time_in_seconds=timedelta(seconds=30*60), # Time after which a phrase can be considered old and need not be tracked.

                                   )
