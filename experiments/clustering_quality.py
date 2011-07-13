'''
Created on Jul 12, 2011

@author: kykamath
'''
import sys, os
from library.clustering import EMTextClustering
sys.path.append('../')
from twitter_streams_clustering import TwitterIterators
from library.file_io import FileIO
from library.nlp import getWordsFromRawEnglishMessage, getPhrases
from settings import experts_twitter_stream_settings

clustering_quality_experts_data_folder = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_data_folder/'
unique_string = ':ilab:'

class GenerateData:
    @staticmethod
    def forLength(length):
        i=0
        fileName=clustering_quality_experts_data_folder+str(length)
        for tweet in TwitterIterators.iterateTweetsFromExperts(): 
            FileIO.writeToFileAsJson(tweet, fileName)
            i+=1
            if i==length: break;
        os.system('gzip %s'%fileName)
        
class TweetsFile:
    def __init__(self, length, **stream_settings):
        self.length=length
        self.stream_settings = stream_settings
        self.fileName = '/mnt/chevron/kykamath/data/twitter/lsh_clustering/clustering_quality_experts_data_folder/'+str(length)
    def iterator(self):
        for tweet in TwitterIterators.iterateFromFile(self.fileName+'.gz'): yield tweet
    def generateStatsForEMClustering(self, numberOfClusters):
        def _getDocumentsFromIterator():
            userMap = {}
            for tweet in self.iterator():
                user = tweet['user']['screen_name']
                phrases = [phrase.replace(' ', unique_string) for phrase in getPhrases(getWordsFromRawEnglishMessage(tweet['text']), self.stream_settings['min_phrase_length'], self.stream_settings['max_phrase_length'])]
                if phrases:
                    if user not in userMap: userMap[user] = ' '.join(phrases)
                    else: userMap[user]+= ' ' + ' '.join(phrases)
            return userMap.iteritems()
        documents = _getDocumentsFromIterator()
        print EMTextClustering(documents,numberOfClusters).cluster()
        
if __name__ == '__main__':
#    [GenerateData.forLength(i*j) for i in [10**3, 10**4, 10**5] for j in range(1, 10)]
    
    tf = TweetsFile(1000, **experts_twitter_stream_settings)
    tf.generateStatsForEMClustering(numberOfClusters=10)
