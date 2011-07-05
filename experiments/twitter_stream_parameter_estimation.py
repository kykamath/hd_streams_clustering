'''
Created on Jul 4, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from settings import experts_twitter_stream_settings
from hd_streams_clustering import DataStreamMethods
from classes import UtilityMethods, Phrase
from library.classes import GeneralMethods
from twitter_streams_clustering import TwitterIterators,\
    TwitterCrowdsSpecificMethods

class EstimateDimensions:
    def __init__(self, **twitter_stream_settings):
        self.twitter_stream_settings = twitter_stream_settings
        self.phraseTextToPhraseObjectMap = {}
        self.convertDataToMessageMethod=twitter_stream_settings['convert_data_to_message_method']
        self.timeUnitInSeconds = twitter_stream_settings['time_unit_in_seconds']
        self.topDimensionsDuringPreviousIteration = None
        
    def run(self, dataIterator):
        for data in dataIterator:
            message = self.convertDataToMessageMethod(data, **self.stream_settings)
            if DataStreamMethods.messageInOrder(message.timeStamp):
                UtilityMethods.updatePhraseTextToPhraseObject(message.vector, message.timeStamp, self.phraseTextToPhraseObjectMap, **self.stream_settings)
                GeneralMethods.callMethodEveryInterval(EstimateDimensions.estimateMaxDimensions, self.timeUnitInSeconds, message.timeStamp, 
                                                       estimateDimensionsObject=self,
                                                       currentMessageTime=message.timeStamp)
    @staticmethod
    def estimateMaxDimensions(estimateDimensionsObject, currentMessageTime):
        print currentMessageTime
#        def updatePhraseScore(phraseObject): 
#            phraseObject.updateScore(currentMessageTime, 0, **estimateDimensionsObject.stream_settings)
#            return phraseObject
#        UtilityMethods.pruneUnnecessaryPhrases(estimateDimensionsObject.phraseTextToPhraseObjectMap, currentMessageTime, UtilityMethods.pruningConditionDeterministic, **estimateDimensionsObject.stream_settings)
#        topDimensionsDuringCurrentIteration = [p.text for p in Phrase.sort((updatePhraseScore(p) for p in estimateDimensionsObject.phraseTextToPhraseObjectMap.itervalues()), reverse=True)]
#        if estimateDimensionsObject.topDimensionsDuringPreviousIteration:
#            pass
#        print topDimensionsDuringCurrentIteration
            
if __name__ == '__main__':
#    ParameterEstimation.estimateMaxDimensions(TwitterIterators.iterateFromFile('/Users/kykamath/data/sample.gz'), **experts_twitter_stream_settings)
    experts_twitter_stream_settings['convert_data_to_message_method'] = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
    EstimateDimensions(**experts_twitter_stream_settings).run(TwitterIterators.iterateTweetsFromExperts())