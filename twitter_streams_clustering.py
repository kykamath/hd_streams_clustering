'''
Created on Jun 22, 2011

@author: kykamath
'''
from settings import experts_twitter_stream_settings, trends_twitter_stream_settings
from library.twitter import TweetFiles, getDateTimeObjectFromTweetTimestamp
from classes import Message
from collections import defaultdict
from datetime import datetime, timedelta
from streaming_lsh.library.file_io import FileIO
from hd_streams_clustering import HDStreaminClustering
from library.nlp import getPhrases, getWordsFromRawEnglishMessage
from library.vector import Vector
from nltk.metrics.distance import jaccard_distance
from streaming_lsh.classes import Cluster
from operator import itemgetter

def getExperts():
    usersList, usersData = {}, defaultdict(list)
    for l in open(experts_twitter_stream_settings.users_to_crawl_file): data = l.strip().split(); usersData[data[0]].append(data[1:])
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
            for tweet in TwitterIterators.iterateFromFile(experts_twitter_stream_settings.twitter_users_tweets_folder+'%s.gz'%FileIO.getFileByDay(currentTime)):
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
    @staticmethod
    def combineClusters(clusters, **twitter_stream_settings):
        def getHashtagSet(vector): return set([word for dimension in vector for word in dimension.split() if word.startswith('#')])
        mergedClustersMap = {}
        for cluster in clusters.itervalues():
            mergedClusterId = None
            for mergedCluster in mergedClustersMap.itervalues():
                clusterHashtags, mergedClusterHashtags = getHashtagSet(cluster), getHashtagSet(mergedCluster)
                if len(clusterHashtags.union(mergedClusterHashtags)) and jaccard_distance(clusterHashtags, mergedClusterHashtags) <= 1-twitter_stream_settings['cluster_merging_jaccard_distance_threshold']: 
                    mergedCluster.mergeCluster(cluster), mergedCluster.mergedClustersList.append(cluster.clusterId)
                    mergedClusterId = mergedCluster.clusterId
                    break
            if mergedClusterId==None:
                mergedCluster = Cluster.getClusterObjectToMergeFrom(cluster)
                mergedCluster.mergedClustersList = [cluster.clusterId]
                mergedClustersMap[mergedCluster.clusterId]=mergedCluster
        return mergedClustersMap
    @staticmethod
    def getClusterInMapFormat(cluster, numberOfMaxDimensionsToRepresent=20): 
        return {'clusterId': cluster.clusterId, 'mergedClustersList': cluster.mergedClustersList, 
               'streams': [stream.docId for stream in cluster.iterateDocumentsInCluster()],
               'dimensions': cluster.getTopDimensions(numberOfFeatures=numberOfMaxDimensionsToRepresent)}
    @staticmethod
    def analyzeIterationData(hdStreamClusteringObject, currentMessageTime):
        print '\n\n\nEntering:', currentMessageTime, len(hdStreamClusteringObject.phraseTextAndDimensionMap), len(hdStreamClusteringObject.phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
        for cluster, _ in sorted(Cluster.iterateByAttribute(hdStreamClusteringObject.clusters.values(), 'length'), key=itemgetter(1), reverse=True)[:1]:
            print cluster.clusterId, cluster.length, [stream.docId for stream in cluster.iterateDocumentsInCluster()][:5], cluster.getTopDimensions(numberOfFeatures=5)
        print 'Leaving: ', currentMessageTime, len(hdStreamClusteringObject.phraseTextAndDimensionMap), len(hdStreamClusteringObject.phraseTextToPhraseObjectMap), len(hdStreamClusteringObject.clusters)
        
def clusterTwitterStreams():
    experts_twitter_stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
    experts_twitter_stream_settings['combine_clusters_method'] = TwitterCrowdsSpecificMethods.combineClusters
    experts_twitter_stream_settings['analyze_iteration_data_method'] = TwitterCrowdsSpecificMethods.analyzeIterationData
    hdsClustering = HDStreaminClustering(**experts_twitter_stream_settings)
    hdsClustering.cluster(TwitterIterators.iterateTweetsFromExperts())
    
#    hdsClustering = HDStreaminClustering(**trends_twitter_stream_settings)
#    hdsClustering.cluster(TwitterIterators.iterateFromFile('/mnt/chevron/kykamath/data/twitter/filter/2011_2_6.gz'), 
#                            TwitterCrowdsSpecificMethods.convertTweetJSONToMessage, 
#                            TwitterCrowdsSpecificMethods.combineClusters, 
#                            TwitterCrowdsSpecificMethods.printClusterInfo)
            
if __name__ == '__main__':
    clusterTwitterStreams()
