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
from library.plotting import getLatexForString, CurveFit
from library.math_modified import getSmallestPrimeNumberGreaterThan
from hd_streams_clustering import DataStreamMethods
from classes import UtilityMethods, Phrase
from matplotlib.ticker import FuncFormatter
from twitter_streams_clustering import TwitterIterators, TwitterCrowdsSpecificMethods
from collections import defaultdict
import matplotlib.pyplot as plt
from datetime import timedelta
 

experts_twitter_stream_settings['convert_data_to_message_method']=houston_twitter_stream_settings['convert_data_to_message_method']=TwitterCrowdsSpecificMethods.convertTweetJSONToMessage

'''
Phrase not touched in 5 minutes came back to dimensions
phases not tocuhed in 10 minutes came back again
and so on.
Find the time after which percentage of phrases returning is constant.
'''
class ParameterEstimation:
    '''
    This class is used to dimensionsEstimation dimensions in the stream. To dimensionsEstimation it we calculate
    the number of phrases that need to added every iteration for different dimensions.
    The dimension at which the number of phrases added stablizes is the number of dimensions
    for the stream.
    
    Why do we need this?
    The aim is to get dimensions, that dont change too often at the same time are not very huge.
    This experiments gives us an approximate idea of the number of dimensions. Randomly picking 
    a small value will result in dimensions that are not good and picking too big a value will 
    result in inefficiency.  
    '''
    dimensionsEstimationId = 'dimensions_estimation'
    dimensionsUpdateFrequencyId = 'dimensions_update_frequency_id'
    def __init__(self, **twitter_stream_settings):
        self.twitter_stream_settings = twitter_stream_settings
        self.phraseTextToPhraseObjectMap = {}
        self.convertDataToMessageMethod=twitter_stream_settings['convert_data_to_message_method']
        self.timeUnitInSeconds = twitter_stream_settings['time_unit_in_seconds']
        self.topDimensionsDuringPreviousIteration = None
        self.dimensionListsMap = {}
        self.boundaries = [50, 100, 500, 1000, 5000]+[10000*i for i in range(1,21)]
        self.dimensionUpdateTimeDeltas = [timedelta(seconds=i*10*60) for i in range(1,19)]
        self.dimensionsEstimationFile = twitter_stream_settings['parameter_estimation_folder']+'dimensions'
        self.dimensionsUpdateFrequencyFile = twitter_stream_settings['parameter_estimation_folder']+'dimensions_update_frequency'
        
    def run(self, dataIterator, estimationMethod):
        for data in dataIterator:
            message = self.convertDataToMessageMethod(data, **self.twitter_stream_settings)
            if DataStreamMethods.messageInOrder(message.timeStamp):
                UtilityMethods.updatePhraseTextToPhraseObject(message.vector, message.timeStamp, self.phraseTextToPhraseObjectMap, **self.twitter_stream_settings)
                GeneralMethods.callMethodEveryInterval(estimationMethod, self.timeUnitInSeconds, message.timeStamp, 
                                                       estimationObject=self,
                                                       currentMessageTime=message.timeStamp)
    def plotGrowthOfPhrasesInTime(self, returnAxisValuesOnly=True):
        '''
        This plot tells us the time when the number of phrases in the stream stablizes. 
        Consider the time after we have seen maximum phrases to determine dimensions.
        But, if these phrases increase linearly with time, it shows that we have infinte
        dimensions and hence this motivates us to have a way to determine number of 
        dimensions.
        '''
        x, y = [], []; [(x.append(getDateTimeObjectFromTweetTimestamp(line['time_stamp'])),y.append(line['total_number_of_phrases'])) for line in FileIO.iterateJsonFromFile(self.dimensionsEstimationFile)]
#        x=x[:1500]; y=y[:1500]
        plt.subplot(111).yaxis.set_major_formatter(FuncFormatter(lambda x,i: '%0.1f'%(x/10.**6)))
        plt.text(0.0, 1.01, getLatexForString('10^6'), transform = plt.gca().transAxes)
        plt.ylabel(getLatexForString('\# of dimensions')), plt.xlabel(getLatexForString('Time units')), plt.title(getLatexForString('Growth in dimensions with increasing time.'))
        plt.plot(y, color=self.twitter_stream_settings['plot_color'], label=getLatexForString(self.twitter_stream_settings['plot_label']), lw=2)
        plt.legend(loc=4)
        if returnAxisValuesOnly: plt.show()
    def plotDimensionsEstimation(self, returnAxisValuesOnly=True):
        dataDistribution = defaultdict(list)
        for line in FileIO.iterateJsonFromFile(self.dimensionsEstimationFile):
            for k, v in line[ParameterEstimation.dimensionsEstimationId].iteritems():
                k=int(k)
                if k not in dataDistribution: dataDistribution[k]=[0.,0.]
                dataDistribution[k][0]+=v; dataDistribution[k][1]+=1
        x, y = [], []; [(x.append(k), y.append((dataDistribution[k][0]/dataDistribution[k][1])/k)) for k in sorted(dataDistribution) if k>1000] 
        exponentialCurveParams = CurveFit.getParamsForExponentialFitting(x, y)
        print self.twitter_stream_settings['plot_label'], exponentialCurveParams, ParameterEstimation.calculateDimensionsFor(exponentialCurveParams, 0.01) 
        plt.ylabel(getLatexForString('\% of new dimensions')), plt.xlabel(getLatexForString('\# of dimensions')), plt.title(getLatexForString('Dimension stability with increasing number of dimensions.'))
        plt.semilogy(x,y,'o', color=self.twitter_stream_settings['plot_color'], label=getLatexForString(self.twitter_stream_settings['plot_label'])+getLatexForString(' (%0.2fx^{-%0.2f})')%(exponentialCurveParams[0], exponentialCurveParams[1]), lw=2)
        plt.semilogy(x, CurveFit.getYValuesForExponential(exponentialCurveParams, x), color=self.twitter_stream_settings['plot_color'], lw=2)
        plt.legend()
        if returnAxisValuesOnly: plt.show()
    @staticmethod
    def calculateDimensionsFor(params, percentageOfNewDimensions): 
        '''
        Experts stream: ParameterEstimation.calculateDimensionsFor([  1.09194452e+03,   1.03448106e+00], 0.01) = 74177
        Houston stream: ParameterEstimation.calculateDimensionsFor([  2.18650595e+03,   1.02834433e+00], 0.01) = 155801
        '''
        print getSmallestPrimeNumberGreaterThan(int(CurveFit.inverseExponentialFunction(params, percentageOfNewDimensions)))
    @staticmethod
    def plotMethods(methods): map(lambda method: method(returnAxisValuesOnly=False), methods), plt.show()
    @staticmethod
    def dimensionsEstimation(estimationObject, currentMessageTime):
        def updatePhraseScore(phraseObject): 
            phraseObject.updateScore(currentMessageTime, 0, **estimationObject.twitter_stream_settings)
            return phraseObject
        topDimensionsDuringCurrentIteration = [p.text for p in Phrase.sort((updatePhraseScore(p) for p in estimationObject.phraseTextToPhraseObjectMap.itervalues()), reverse=True)]
        oldList, newList = estimationObject.topDimensionsDuringPreviousIteration, topDimensionsDuringCurrentIteration
        if estimationObject.topDimensionsDuringPreviousIteration:
            dimensions_estimation = {}
            for boundary in estimationObject.boundaries:
                if boundary<len(estimationObject.phraseTextToPhraseObjectMap): dimensions_estimation[str(boundary)]=len(set(newList[:boundary]).difference(oldList[:boundary]))
            print currentMessageTime, len(estimationObject.phraseTextToPhraseObjectMap)
            iterationData = {
                             'time_stamp': getStringRepresentationForTweetTimestamp(currentMessageTime),
                             'total_number_of_phrases': len(estimationObject.phraseTextToPhraseObjectMap),
                             'settings': estimationObject.twitter_stream_settings.convertToSerializableObject(),
                             ParameterEstimation.dimensionsEstimationId:dimensions_estimation
                             }
            FileIO.writeToFileAsJson(iterationData, estimationObject.dimensionsEstimationFile)
        estimationObject.topDimensionsDuringPreviousIteration=topDimensionsDuringCurrentIteration[:]
    @staticmethod
    def dimensionsUpdateFrequencyEstimation(estimationObject, currentMessageTime):
        def updatePhraseScore(phraseObject): 
            phraseObject.updateScore(currentMessageTime, 0, **estimationObject.twitter_stream_settings)
            return phraseObject
        dimensions=estimationObject.twitter_stream_settings['dimensions']
        newList = [p.text for p in Phrase.sort((updatePhraseScore(p) for p in estimationObject.phraseTextToPhraseObjectMap.itervalues()), reverse=True)][:dimensions]
        print currentMessageTime, len(newList)
        if len(newList)>=dimensions:
            idsOfDimensionsListToCompare = [(i, GeneralMethods.approximateToNearest5Minutes(currentMessageTime-i)) for i in estimationObject.dimensionUpdateTimeDeltas if GeneralMethods.approximateToNearest5Minutes(currentMessageTime-i) in estimationObject.dimensionListsMap]
            dimensionsUpdateFrequency = {}
            for td, id in idsOfDimensionsListToCompare:
                oldList = estimationObject.dimensionListsMap[id]
                dimensionsUpdateFrequency[str(td.seconds)]=len(set(newList).difference(oldList))
            print len(estimationObject.dimensionListsMap), currentMessageTime, len(newList), [(k,dimensionsUpdateFrequency[k]) for k in sorted(dimensionsUpdateFrequency)]
            iterationData = {
                             'time_stamp': getStringRepresentationForTweetTimestamp(currentMessageTime),
                             'total_number_of_phrases': len(estimationObject.phraseTextToPhraseObjectMap),
                             'settings': pprint.pformat(estimationObject.twitter_stream_settings),
                             ParameterEstimation.dimensionsEstimationId:dimensionsUpdateFrequency
                             }
            FileIO.writeToFileAsJson(iterationData, estimationObject.dimensionsUpdateFrequencyFile)
            estimationObject.dimensionListsMap[GeneralMethods.approximateToNearest5Minutes(currentMessageTime)] = newList[:]
            for key in estimationObject.dimensionListsMap.keys()[:]:
                if currentMessageTime-key > estimationObject.dimensionUpdateTimeDeltas[-1]: del estimationObject.dimensionListsMap[key]

def dimensionsEstimation():
#    ParameterEstimation(**experts_twitter_stream_settings).run(TwitterIterators.iterateTweetsFromExperts(), ParameterEstimation.dimensionsEstimation)
#    ParameterEstimation(**houston_twitter_stream_settings).run(TwitterIterators.iterateTweetsFromHouston(), ParameterEstimation.dimensionsEstimation)
    ParameterEstimation.plotMethods([ParameterEstimation(**experts_twitter_stream_settings).plotGrowthOfPhrasesInTime, ParameterEstimation(**houston_twitter_stream_settings).plotGrowthOfPhrasesInTime])

def dimensionsUpdateFrequencyEstimation():
    ParameterEstimation(**experts_twitter_stream_settings).run(TwitterIterators.iterateTweetsFromExperts(), ParameterEstimation.dimensionsUpdateFrequencyEstimation)
    
if __name__ == '__main__':
#    dimensionsEstimation()
    dimensionsUpdateFrequencyEstimation()
