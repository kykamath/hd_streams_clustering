'''
Created on Jun 22, 2011

@author: kykamath
'''
from settings import twitter_stream_settings
from library.twitter import TweetFiles, getDateTimeObjectFromTweetTimestamp
from classes import Message, UtilityMethods
from collections import defaultdict
from datetime import datetime, timedelta
from streaming_lsh.library.file_io import FileIO
from hd_streams_clustering import HDStreaminClustering
from library.nlp import getPhrases, getWordsFromRawEnglishMessage
from library.vector import Vector

def getExperts():
    usersList, usersData = {}, defaultdict(list)
    for l in open(twitter_stream_settings.usersToCrawl): data = l.strip().split(); usersData[data[0]].append(data[1:])
    for k, v in usersData.iteritems(): 
        for user in v: usersList[user[1]] = {'screen_name': user[0], 'class':k}
    return usersList

class TwitterIterators:
    '''
    Returns a tweet on every iteration. This iterator will be used to cluster streams.
    To use this with adifferent data soruce, ex. Twitter Streaming API, write a iterator for the
    data source accordingly.
    '''
    @staticmethod
    def iterateFromFile(file):
        for tweet in TweetFiles.iterateTweetsFromGzip(file): yield tweet
    @staticmethod
    def iterateTweetsFromExperts(expertsDataStartTime=datetime(2011,3,19), expertsDataEndTime=datetime(2011,4,12)):
        experts = getExperts()
        currentTime = expertsDataStartTime
        while currentTime <= expertsDataEndTime:
            for tweet in TwitterIterators.iterateFromFile(twitter_stream_settings.twitterUsersTweetsFolder+'%s.gz'%FileIO.getFileByDay(currentTime)):
                if tweet['user']['id_str'] in experts: yield tweet
            currentTime+=timedelta(days=1)

class TwitterCrowdsSpecificMethods:
    @staticmethod
    def convertTweetJSONToMessage(tweet, **twitter_stream_settings):
        tweetTime = getDateTimeObjectFromTweetTimestamp(tweet['created_at'])
        message = Message(tweet['user']['screen_name'], tweet['id'], tweet['text'], tweetTime)
        message.vector = Vector()
        for phrase in getPhrases(getWordsFromRawEnglishMessage(tweet['text']), twitter_stream_settings['min_phrase_length'], twitter_stream_settings['max_phrase_length']):
            if phrase not in message.vector: message.vector[phrase]=0
            message.vector[phrase]+=1
        return message

def clusterTwitterStreams():
    hdsClustering = HDStreaminClustering(**twitter_stream_settings)
#    hdsClustering.cluster(TwitterIterators.iterateFromFile('/mnt/chevron/kykamath/temp_data/sample.gz'), TwitterCrowdsSpecificMethods.convertTweetJSONToMessage)
    hdsClustering.cluster(TwitterIterators.iterateTweetsFromExperts(), TwitterCrowdsSpecificMethods.convertTweetJSONToMessage)
#    hdsClustering.cluster(TwitterIterators.iterateFromFile('/mnt/chevron/kykamath/data/twitter/filter/2011_2_6.gz'), TwitterCrowdsSpecificMethods.convertTweetJSONToMessage)
            
if __name__ == '__main__':
    clusterTwitterStreams()