'''
Created on Jul 4, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
import pprint
from settings import experts_twitter_stream_settings, houston_twitter_stream_settings
from library.file_io import FileIO
from library.classes import GeneralMethods
from library.twitter import getStringRepresentationForTweetTimestamp, getDateTimeObjectFromTweetTimestamp
from hd_streams_clustering import DataStreamMethods
from classes import UtilityMethods, Phrase
from twitter_streams_clustering import TwitterIterators, TwitterCrowdsSpecificMethods
from collections import defaultdict
import matplotlib.pyplot as plt

class Dimensions:
    '''
    This class is used to estimate dimensions in the stream. To estimate it we calculate
    the number of phrases that need to added every iteration for different dimensions.
    The dimension at which the number of phrases added stablizes is the number of dimensions
    for the stream.
    
    Why do we need this?
    The aim is to get dimensions, that dont change too often at the same time are not very huge.
    This experiments gives us an approximate idea of the number of dimensions. Randomly picking 
    a small value will result in dimensions that are not good and picking too big a value will 
    result in inefficiency.  
    '''
    id = 'dimensions_estimation'
    def __init__(self, **twitter_stream_settings):
        self.twitter_stream_settings = twitter_stream_settings
        self.phraseTextToPhraseObjectMap = {}
        self.convertDataToMessageMethod=twitter_stream_settings['convert_data_to_message_method']
        self.timeUnitInSeconds = twitter_stream_settings['time_unit_in_seconds']
        self.topDimensionsDuringPreviousIteration = None
        self.boundaries = [50, 100, 500, 1000, 5000]+[10000*i for i in range(1,21)]
        self.dimensionsEstimationFile = twitter_stream_settings['parameter_estimation_folder']+'dimensions'
        
    def run(self, dataIterator):
        for data in dataIterator:
            message = self.convertDataToMessageMethod(data, **self.twitter_stream_settings)
            if DataStreamMethods.messageInOrder(message.timeStamp):
                UtilityMethods.updatePhraseTextToPhraseObject(message.vector, message.timeStamp, self.phraseTextToPhraseObjectMap, **self.twitter_stream_settings)
                GeneralMethods.callMethodEveryInterval(Dimensions.estimate, self.timeUnitInSeconds, message.timeStamp, 
                                                       estimateDimensionsObject=self,
                                                       currentMessageTime=message.timeStamp)
    def plotEstimate(self):
        dataDistribution = defaultdict(list)
        for line in FileIO.iterateJsonFromFile(self.dimensionsEstimationFile):
            for k, v in line[Dimensions.id].iteritems():
                k=int(k)
                if k not in dataDistribution: dataDistribution[k]=[0.,0.]
                dataDistribution[k][0]+=v; dataDistribution[k][1]+=1
        x, y = [], []
        for k in sorted(dataDistribution): x.append(k), y.append((dataDistribution[k][0]/dataDistribution[k][1])/k)
        plt.plot(x,y)
        plt.show()
    def plotGrowthOfPhraseInTime(self):
        for line in FileIO.iterateJsonFromFile(self.dimensionsEstimationFile):
            print line['time_stamp'], line['total_number_of_phrases']
    @staticmethod
    def estimate(estimateDimensionsObject, currentMessageTime):
        def updatePhraseScore(phraseObject): 
            phraseObject.updateScore(currentMessageTime, 0, **estimateDimensionsObject.twitter_stream_settings)
            return phraseObject
        topDimensionsDuringCurrentIteration = [p.text for p in Phrase.sort((updatePhraseScore(p) for p in estimateDimensionsObject.phraseTextToPhraseObjectMap.itervalues()), reverse=True)]
        oldList, newList = estimateDimensionsObject.topDimensionsDuringPreviousIteration, topDimensionsDuringCurrentIteration
        if estimateDimensionsObject.topDimensionsDuringPreviousIteration:
            dimensions_estimation = {}
            for boundary in estimateDimensionsObject.boundaries:
                if boundary<len(estimateDimensionsObject.phraseTextToPhraseObjectMap): dimensions_estimation[str(boundary)]=len(set(newList[:boundary]).difference(oldList[:boundary]))
            print currentMessageTime, len(estimateDimensionsObject.phraseTextToPhraseObjectMap)
            iterationData = {
                             'time_stamp': getStringRepresentationForTweetTimestamp(currentMessageTime),
                             'total_number_of_phrases': len(estimateDimensionsObject.phraseTextToPhraseObjectMap),
                             'settings': experts_twitter_stream_settings.convertToSerializableObject(),
                             Dimensions.id:dimensions_estimation
                             }
            FileIO.writeToFileAsJson(iterationData, estimateDimensionsObject.dimensionsEstimationFile)
        estimateDimensionsObject.topDimensionsDuringPreviousIteration=topDimensionsDuringCurrentIteration[:]

def estimateParametersForExpertsStream():
    experts_twitter_stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
    Dimensions(**experts_twitter_stream_settings).run(TwitterIterators.iterateTweetsFromExperts())
#    Dimensions(**experts_twitter_stream_settings).plotGrowthOfPhraseInTime()

def estimateParametersForHoustonStream():
    houston_twitter_stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
    Dimensions(**houston_twitter_stream_settings).run(TwitterIterators.iterateTweetsFromHouston())
#    Dimensions(**houston_twitter_stream_settings).plotEstimate()
    
if __name__ == '__main__':
    estimateParametersForExpertsStream()
#    estimateParametersForHoustonStream()