'''
Created on Jul 12, 2011

@author: kykamath
'''
import sys, os, time
from library.clustering import KMeansClustering, EvaluationMetrics
sys.path.append('../')
from twitter_streams_clustering import TwitterIterators, getExperts
from library.file_io import FileIO
from library.nlp import getWordsFromRawEnglishMessage, getPhrases
from settings import experts_twitter_stream_settings
from itertools import groupby
from operator import itemgetter

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
    def generateStatsForKMeansClustering(self, numberOfClusters):
        ts = time.time()
        def _getDocumentsFromIterator():
            userMap = {}
            for tweet in self.iterator():
                user = tweet['user']['screen_name']
                phrases = [phrase.replace(' ', unique_string) for phrase in getPhrases(getWordsFromRawEnglishMessage(tweet['text']), self.stream_settings['min_phrase_length'], self.stream_settings['max_phrase_length'])]
                if phrases:
                    if user not in userMap: userMap[user] = ' '.join(phrases)
                    else: userMap[user]+= ' ' + ' '.join(phrases)
            return userMap.iteritems()
        documents = list(_getDocumentsFromIterator())
        clusters = KMeansClustering(documents,len(documents)).cluster()
        te = time.time()
        documentClusters = []
        for a in [ (k, list(v)) for k,v in groupby(sorted((a[0], a[1][0]) for a in zip(clusters, documents)), key=itemgetter(0))]:
            if len(a[1]) >= 2: documentClusters.append(zip(*a[1])[1])
        expertsToClassMap = dict([(k, v['class']) for k,v in getExperts(byScreenName=True).iteritems()])
        def getExpertClasses(cluster): return [expertsToClassMap[user.lower()] for user in cluster if user.lower() in expertsToClassMap]
        clustersForEvaluation = [getExpertClasses(cluster) for cluster in documentClusters]
        iterationData =  {'no_of_documents':self.length, 'no_of_clusters': len(documentClusters), 'iteration_time': te-ts}
        iterationData['nmi'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.nmi)
        iterationData['purity'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.purity)
        iterationData['f1'] = EvaluationMetrics.getValueForClusters(clustersForEvaluation, EvaluationMetrics.f1)
        return iterationData
    def generateStatsForStreamingLSHClustering(self):
        
    
if __name__ == '__main__':
#    [GenerateData.forLength(i*j) for i in [10**3, 10**4, 10**5] for j in range(1, 10)]
    
    tf = TweetsFile(4000, **experts_twitter_stream_settings)
    tf.generateStatsForKMeansClustering(numberOfClusters=2)
