'''
Created on Jun 22, 2011

@author: kykamath
'''
from library.classes import Settings

twitter_stream_settings = Settings(
                                   min_phrase_length=1,
                                   max_phrase_length=1
                                   )

print twitter_stream_settings['key']