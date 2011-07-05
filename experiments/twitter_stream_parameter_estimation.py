'''
Created on Jul 4, 2011

@author: kykamath
'''
import sys
from matplotlib.dates import AutoDateLocator
from matplotlib.ticker import ScalarFormatter, FuncFormatter
from library.plotting import getLatexForString, CurveFit
from library.math_modified import getSmallestPrimeNumberGreaterThan
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
 
 
'''
Phrase not touched in 5 minutes came back to dimensions
phases not tocuhed in 10 minutes came back again
and so on.
Find the time after which percentage of phrases returning is constant.
'''
 

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
    def plotGrowthOfPhraseInTime(self, returnAxisValuesOnly=True):
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
    def plotEstimate(self, returnAxisValuesOnly=True):
        dataDistribution = defaultdict(list)
        for line in FileIO.iterateJsonFromFile(self.dimensionsEstimationFile):
            for k, v in line[Dimensions.id].iteritems():
                k=int(k)
                if k not in dataDistribution: dataDistribution[k]=[0.,0.]
                dataDistribution[k][0]+=v; dataDistribution[k][1]+=1
        x, y = [], []; [(x.append(k), y.append((dataDistribution[k][0]/dataDistribution[k][1])/k)) for k in sorted(dataDistribution) if k>1000] 
        exponentialCurveParams = CurveFit.getParamsForExponentialFitting(x, y)
        print self.twitter_stream_settings['plot_label'], exponentialCurveParams
        plt.ylabel(getLatexForString('\% of new dimensions')), plt.xlabel(getLatexForString('\# of dimensions')), plt.title(getLatexForString('Dimension stability with increasing number of dimensions.'))
        plt.semilogy(x,y,'o', color=self.twitter_stream_settings['plot_color'], label=getLatexForString(self.twitter_stream_settings['plot_label'])+getLatexForString(' (%0.2fx^{-%0.2f})')%(exponentialCurveParams[0], exponentialCurveParams[1]), lw=2)
        plt.semilogy(x, CurveFit.getYValuesForExponential(exponentialCurveParams, x), color=self.twitter_stream_settings['plot_color'], lw=2)
        plt.legend()
        if returnAxisValuesOnly: plt.show()
    @staticmethod
    def calculateDimensionsFor(params, percentageOfNewDimensions): 
        '''
        Experts stream: Dimensions.calculateDimensionsFor([  1.09194452e+03,   1.03448106e+00], 0.01) = 74177
        Houston stream: Dimensions.calculateDimensionsFor([  2.18650595e+03,   1.02834433e+00], 0.01) = 155801

        '''
        print getSmallestPrimeNumberGreaterThan(int(CurveFit.inverseExponentialFunction(params, percentageOfNewDimensions)))
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
    @staticmethod
    def plotMethods(methods): map(lambda method: method(returnAxisValuesOnly=False), methods), plt.show()

def estimateParametersForExpertsStream(): pass
def estimateParametersForHoustonStream(): pass
    
if __name__ == '__main__':
    experts_twitter_stream_settings['convert_data_to_message_method']=houston_twitter_stream_settings['convert_data_to_message_method']=TwitterCrowdsSpecificMethods.convertTweetJSONToMessage
    Dimensions.plotMethods([Dimensions(**experts_twitter_stream_settings).plotEstimate, Dimensions(**houston_twitter_stream_settings).plotEstimate])
