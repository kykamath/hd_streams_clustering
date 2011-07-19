'''
Created on Jul 4, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
import pprint
from settings import experts_twitter_stream_settings, houston_twitter_stream_settings
from library.file_io import FileIO
from library.classes import GeneralMethods, FixedIntervalMethod
from library.twitter import getStringRepresentationForTweetTimestamp, getDateTimeObjectFromTweetTimestamp
from library.plotting import getLatexForString, CurveFit,\
    getCumulativeDistribution
from library.math_modified import getSmallestPrimeNumberGreaterThan,\
    DateTimeAirthematic
from hd_streams_clustering import DataStreamMethods, HDStreaminClustering
from classes import UtilityMethods, Phrase
from matplotlib.ticker import FuncFormatter
from twitter_streams_clustering import TwitterIterators, TwitterCrowdsSpecificMethods
from quality_comparison_with_kmeans import TweetsFile as KMeansTweetsFile
from collections import defaultdict
import matplotlib.pyplot as plt
from datetime import timedelta
from operator import itemgetter
import numpy as np

numberOfTimeUnits=10*24*12
xlabelTimeUnits = 'Time units'

class ParameterEstimation:
    dimensionsEstimationId = 'dimensions_estimation'
    dimensionsUpdateFrequencyId = 'dimensions_update_frequency_id'
    dimensionInActivityTimeId = 'dimension_inactivity_time_id'
    def __init__(self, **stream_settings):
        self.stream_settings = stream_settings
        self.phraseTextToPhraseObjectMap = {}
        self.convertDataToMessageMethod=stream_settings['convert_data_to_message_method']
        self.timeUnitInSeconds = stream_settings['time_unit_in_seconds']
        self.topDimensionsDuringPreviousIteration = None
        self.dimensionListsMap = {}
        self.boundaries = [50, 100, 500, 1000, 5000]+[10000*i for i in range(1,21)]
        self.dimensionUpdateTimeDeltas = [timedelta(seconds=i*10*60) for i in range(1,31)]
        self.dimensionsEstimationFile = stream_settings['parameter_estimation_folder']+'dimensions'
        self.dimensionsUpdateFrequencyFile = stream_settings['parameter_estimation_folder']+'dimensions_update_frequency'
        self.dimensionInActivityTimeFile = stream_settings['parameter_estimation_folder']+'dimension_inactivity_time'
        self.lagBetweenMessagesDistribution = defaultdict(int)
        
    def run(self, dataIterator, estimationMethod, parameterSpecificDataCollectionMethod=None):
        estimationMethod = FixedIntervalMethod(estimationMethod, self.timeUnitInSeconds)
        for data in dataIterator:
            message = self.convertDataToMessageMethod(data, **self.stream_settings)
            if DataStreamMethods.messageInOrder(message.timeStamp):
                if parameterSpecificDataCollectionMethod!=None: parameterSpecificDataCollectionMethod(estimationObject=self, message=message)
                UtilityMethods.updatePhraseTextToPhraseObject(message.vector, message.timeStamp, self.phraseTextToPhraseObjectMap, **self.stream_settings)
                estimationMethod.call(message.timeStamp, estimationObject=self, currentMessageTime=message.timeStamp)
    @staticmethod
    def dimensionsEstimation(estimationObject, currentMessageTime):
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
        def updatePhraseScore(phraseObject): 
            phraseObject.updateScore(currentMessageTime, 0, **estimationObject.stream_settings)
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
                             'settings': estimationObject.stream_settings.convertToSerializableObject(),
                             ParameterEstimation.dimensionsEstimationId:dimensions_estimation
                             }
            FileIO.writeToFileAsJson(iterationData, estimationObject.dimensionsEstimationFile)
        estimationObject.topDimensionsDuringPreviousIteration=topDimensionsDuringCurrentIteration[:]
    @staticmethod
    def dimensionsUpdateFrequencyEstimation(estimationObject, currentMessageTime):
        '''
        Observe the new dimensions that get added to current dimension if the dimensions 
        are being updated at regular intervals.
        For example, number of dimensions being added after 10m, 20m,... 5 horus. 
        As time increases the number of 'decayed' dimensions increase. The current dimensions
        has a lot of unwanted decayed dimensions. Using this information identify the time 
        interval that is best suited to refresh dimensions. 
        Tentative: We decide to pick the time interval at which the rate of decay is maximum.
        '''
        def updatePhraseScore(phraseObject): 
            phraseObject.updateScore(currentMessageTime, 0, **estimationObject.stream_settings)
            return phraseObject
        dimensions=estimationObject.stream_settings['dimensions']
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
                             'settings': pprint.pformat(estimationObject.stream_settings),
                              ParameterEstimation.dimensionsUpdateFrequencyId:dimensionsUpdateFrequency
                             }
            FileIO.writeToFileAsJson(iterationData, estimationObject.dimensionsUpdateFrequencyFile)
            estimationObject.dimensionListsMap[GeneralMethods.approximateToNearest5Minutes(currentMessageTime)] = newList[:]
            for key in estimationObject.dimensionListsMap.keys()[:]:
                if currentMessageTime-key > estimationObject.dimensionUpdateTimeDeltas[-1]: del estimationObject.dimensionListsMap[key]
    @staticmethod
    def dimensionInActivityTimeEstimation(estimationObject, currentMessageTime):
        phrasesLagDistribution = defaultdict(int)
        for phraseObject in estimationObject.phraseTextToPhraseObjectMap.itervalues():
            lag=DateTimeAirthematic.getDifferenceInTimeUnits(currentMessageTime, phraseObject.latestOccuranceTime, estimationObject.stream_settings['time_unit_in_seconds'].seconds)
            phrasesLagDistribution[str(lag)]+=1
        print currentMessageTime
        iterationData = {
                         'time_stamp': getStringRepresentationForTweetTimestamp(currentMessageTime),
                         'settings': pprint.pformat(estimationObject.stream_settings),
                         ParameterEstimation.dimensionInActivityTimeId:estimationObject.lagBetweenMessagesDistribution,
                         'phrases_lag_distribution': phrasesLagDistribution
                         }
        FileIO.writeToFileAsJson(iterationData, estimationObject.dimensionInActivityTimeFile)
    def plotGrowthOfPhrasesInTime(self, returnAxisValuesOnly=True):
        '''
        This plot tells us the time when the number of phrases in the stream stablizes. 
        Consider the time after we have seen maximum phrases to determine dimensions.
        But, if these phrases increase linearly with time, it shows that we have infinte
        dimensions and hence this motivates us to have a way to determine number of 
        dimensions.
        
        numberOfTimeUnits=10*24*12
        '''
        x, y = [], []; [(x.append(getDateTimeObjectFromTweetTimestamp(line['time_stamp'])),y.append(line['total_number_of_phrases'])) for line in FileIO.iterateJsonFromFile(self.dimensionsEstimationFile)]
        x=x[:numberOfTimeUnits]; y=y[:numberOfTimeUnits]
        plt.subplot(111).yaxis.set_major_formatter(FuncFormatter(lambda x,i: '%0.1f'%(x/10.**6)))
        plt.text(0.0, 1.01, getLatexForString('10^6'), transform = plt.gca().transAxes)
        plt.ylabel(getLatexForString('\# of dimensions')), plt.xlabel(getLatexForString(xlabelTimeUnits)), plt.title(getLatexForString('Growth in dimensions with increasing time.'))
        plt.plot(y, color=self.stream_settings['plot_color'], label=getLatexForString(self.stream_settings['plot_label']), lw=2)
        plt.legend(loc=4)
        if returnAxisValuesOnly: plt.show()
    def plotDimensionsEstimation(self, returnAxisValuesOnly=True):
        def calculateDimensionsFor(params, percentageOfNewDimensions): 
            '''
            numberOfTimeUnits=10*24*12
            Experts stream [  1.17707899e+03   1.03794580e+00] 76819
            Houston stream [  2.73913900e+03   1.02758516e+00] 195731
            '''
            print getSmallestPrimeNumberGreaterThan(int(CurveFit.inverseOfDecreasingExponentialFunction(params, percentageOfNewDimensions)))
        dataDistribution = defaultdict(list)
        for line in FileIO.iterateJsonFromFile(self.dimensionsEstimationFile):
            for k, v in line[ParameterEstimation.dimensionsEstimationId].iteritems():
                k=int(k)
                if k not in dataDistribution: dataDistribution[k]=[0.,0.]
                dataDistribution[k][0]+=v; dataDistribution[k][1]+=1
        x, y = [], []; [(x.append(k), y.append((dataDistribution[k][0]/dataDistribution[k][1])/k)) for k in sorted(dataDistribution) if k>1000]
        x,y=x[:numberOfTimeUnits], y[:numberOfTimeUnits]
        exponentialCurveParams = CurveFit.getParamsAfterFittingData(x, y, CurveFit.decreasingExponentialFunction, [1.,1.])
        print self.stream_settings['plot_label'], exponentialCurveParams, calculateDimensionsFor(exponentialCurveParams, 0.01) 
        plt.ylabel(getLatexForString('\% of new dimensions')), plt.xlabel(getLatexForString('\# of dimensions')), plt.title(getLatexForString('Dimension stability with increasing number of dimensions.'))
        plt.semilogy(x,y,'o', color=self.stream_settings['plot_color'], label=getLatexForString(self.stream_settings['plot_label'])+getLatexForString(' (%0.2fx^{-%0.2f})')%(exponentialCurveParams[0], exponentialCurveParams[1]), lw=2)
        plt.semilogy(x,CurveFit.getYValues(CurveFit.decreasingExponentialFunction, exponentialCurveParams, x), color=self.stream_settings['plot_color'], lw=2)
        plt.legend()
        if returnAxisValuesOnly: plt.show()
    def plotDimensionsUpdateFrequencyEstimation(self, returnAxisValuesOnly=True):
        '''
        numberOfTimeUnits=10*24*12
        Experts stream 12
        Houston stream 2
        '''
        dataDistribution = defaultdict(list)
        for line in FileIO.iterateJsonFromFile(self.dimensionsUpdateFrequencyFile):
            for k, v in line[ParameterEstimation.dimensionsUpdateFrequencyId].iteritems():
                k=int(k)/self.timeUnitInSeconds.seconds
                if k not in dataDistribution: dataDistribution[k]=[0.,0.]
                dataDistribution[k][0]+=v; dataDistribution[k][1]+=1
        x, y = [], []; [(x.append(k), y.append((dataDistribution[k][0]/dataDistribution[k][1]))) for k in sorted(dataDistribution)]
        x1, y1 = [], []; [(x1.append(k), y1.append((dataDistribution[k][0]/dataDistribution[k][1])/k)) for k in sorted(dataDistribution)]
        x=x[:numberOfTimeUnits]; y=y[:numberOfTimeUnits]; x1=x1[:numberOfTimeUnits]; y1=y1[:numberOfTimeUnits]
        def subPlot(id):
            plt.subplot(id)
            inactivityCorordinates = max(zip(x1,y1),key=itemgetter(1))
            plt.semilogx(x1,y1,'-', color=self.stream_settings['plot_color'], label=getLatexForString(self.stream_settings['plot_label'] + ' (Update frequency=%d TU)'%inactivityCorordinates[0]), lw=2)
            plt.subplot(id).yaxis.set_major_formatter(FuncFormatter(lambda x,i: '%0.1f'%(x/10.**3)))
            plt.semilogx([inactivityCorordinates[0]], [inactivityCorordinates[1]], 'o', alpha=0.7, color='r')
            plt.subplot(id).yaxis.set_major_formatter(FuncFormatter(lambda x,i: '%0.1f'%(x/10.**3)))
            plt.yticks((min(y1), max(y1)))
            print self.stream_settings['plot_label'], inactivityCorordinates[0]
        plt.subplot(311)
        plt.title(getLatexForString('Dimensions update frequency estimation'))
        plt.semilogx(x,y,'-', color=self.stream_settings['plot_color'], label=getLatexForString(self.stream_settings['plot_label']), lw=2)
        plt.subplot(311).yaxis.set_major_formatter(FuncFormatter(lambda x,i: '%0.1f'%(x/10.**5)))
        plt.text(0.0, 1.01, getLatexForString('10^5'), transform = plt.gca().transAxes)
        plt.ylabel(getLatexForString('\# of decayed dimensions'))
        if self.stream_settings['stream_id']=='experts_twitter_stream': subPlot(312)
        else: subPlot(313); plt.xlabel(getLatexForString(xlabelTimeUnits))
        plt.ylabel(getLatexForString('Rate of DD (10^3)'))
        plt.legend(loc=3)
        if returnAxisValuesOnly: plt.show()
    def plotCDFDimensionsLagDistribution(self, returnAxisValuesOnly=True):
        '''
        Inactivity time is the time after which there is a high probability that a
        dimension will not appear. Find time_unit that gives this probability. 
        
        Cumulative distribution function (http://en.wikipedia.org/wiki/Cumulative_distribution_function)
        lag = time betweeen occurance of two dimensions (similar to inactivty_time)
        
        F(time_unit) = P(lag<=time_unit)
        time_unit = F_inv(P(lag<=time_unit))
        
        Given P(inactivty_time>time_unit) determine time_unit as shown:
        P(inactivty_time<=time_unit) = 1 - P(inactivty_time>time_unit)
        inactivty_time = F_inv(P(inactivty_time<=time_unit))
        
        numberOfTimeUnits=10*24*12
        
        Experts stream [ 0.23250341  0.250209  ] 0.25 107
        Houston stream [ 0.16948096  0.30751358] 0.25 126
        
        Experts stream [ 0.23250341  0.250209  ] 0.1, 223
        Houston stream [ 0.16948096  0.30751358] 0.1, 228
        
        Compared to other vaues these values are pretty close to each
        other. This is expected. Irrespective of size of the streams,
        the phrases have the same lifetime and hence decay close to each other.
        '''
        def calculateInActivityTimeFor(params, probabilityOfInactivity): return int(CurveFit.inverseOfIncreasingExponentialFunction(params, 1-probabilityOfInactivity))
        data = list(FileIO.iterateJsonFromFile(self.dimensionInActivityTimeFile))[numberOfTimeUnits]
        total = float(sum(data[ParameterEstimation.dimensionInActivityTimeId].values()))
        x = sorted(map(int, data[ParameterEstimation.dimensionInActivityTimeId].keys()))
        y = getCumulativeDistribution([data[ParameterEstimation.dimensionInActivityTimeId][str(i)]/total for i in x])
        print len(x)
        exponentialCurveParams = CurveFit.getParamsAfterFittingData(x, y, CurveFit.increasingExponentialFunction, [1., 1.])
        print self.stream_settings['plot_label'], exponentialCurveParams, calculateInActivityTimeFor(exponentialCurveParams, 0.1) 
        plt.plot(x,y, 'o', label=getLatexForString(self.stream_settings['plot_label'])+getLatexForString(' (%0.2fx^{%0.2f})')%(exponentialCurveParams[0], exponentialCurveParams[1]), color=self.stream_settings['plot_color'])
        plt.plot(x,CurveFit.getYValues(CurveFit.increasingExponentialFunction, exponentialCurveParams, x), color=self.stream_settings['plot_color'], lw=2)
        plt.ylabel(r'$P\ (\ lag\ \leq\ TU\ )$'), plt.xlabel(getLatexForString(xlabelTimeUnits)), plt.title(getLatexForString('CDF for dimension lag distribution.'))
        plt.ylim((0, 1.2))
        plt.legend(loc=4)
        if returnAxisValuesOnly: plt.show()
    def plotPercentageOfDimensionsWithinALag(self, returnAxisValuesOnly=True):
        '''
        This gives us the percentage of phrases we can loose everytime we prune phrases.
        
        Measures the percentage of dimensions having lag less than TU.
        
        So at the end of 10th day, almost y% of phrases can be removed. With some probabiity
        that it will not occure again.
        
        numberOfTimeUnits=10*24*12
        With 75% probability.
        Experts stream [ 0.0097055   0.81888514] 107 0.554497397565
        Houston stream [ 0.00943499  0.825918  ] 126 0.487757815615
        With 90% probability.
        Experts stream [ 0.0097055   0.81888514] 223 0.187150798756
        Houston stream [ 0.00943499  0.825918  ] 228 0.164007589276
        '''
        def calculatePercentageOfDecayedPhrasesFor(params, timeUnit): return 1- CurveFit.increasingExponentialFunction(params, timeUnit)
        dataDistribution = {}
        currentTimeUnit = 0
        for data in list(FileIO.iterateJsonFromFile(self.dimensionInActivityTimeFile))[:numberOfTimeUnits]:
            totalDimensions=float(sum(data['phrases_lag_distribution'].values()))
            tempArray = []
            for k, v in data['phrases_lag_distribution'].iteritems():
                k=int(k)
                if k not in dataDistribution: dataDistribution[k]=[0]*numberOfTimeUnits
                dataDistribution[k][currentTimeUnit] = v/totalDimensions
                tempArray.append(v/totalDimensions)
            currentTimeUnit+=1
        x = sorted(dataDistribution)
        y = getCumulativeDistribution([np.mean(dataDistribution[k]) for k in x])
        params = CurveFit.getParamsAfterFittingData(x, y, CurveFit.increasingExponentialFunction, [1.,1.])
        print self.stream_settings['plot_label'], params, 
        def subPlot(id, timeUnit):
            plt.subplot(id)
            print timeUnit, calculatePercentageOfDecayedPhrasesFor(params, timeUnit)
            plt.plot(x,y, 'o', label=getLatexForString(self.stream_settings['plot_label'])+getLatexForString(' (%0.2fx^{%0.2f})')%(params[0], params[1]), color=self.stream_settings['plot_color'])
            plt.plot(x, CurveFit.getYValues(CurveFit.increasingExponentialFunction, params, x), color=self.stream_settings['plot_color'], lw=2)
        if self.stream_settings['stream_id']=='experts_twitter_stream': subPlot(111, 107); plt.title(getLatexForString('Percentage of phrases within a lag'))
        else: subPlot(111, 126); plt.xlabel(getLatexForString(xlabelTimeUnits))
        plt.ylabel(r'$\%\ of\ phrases\ with\ lag\ \leq\ TU$')
        plt.legend(loc=4)
        if returnAxisValuesOnly: plt.show()
    @staticmethod
    def plotMethods(methods): map(lambda method: method(returnAxisValuesOnly=False), methods), plt.show()
    
class ClusteringParametersEstimation():
    clusterLagDistributionId = 'cluster_lag_distribution'
    def __init__(self, **stream_settings):
        stream_settings['%s_file'%ClusteringParametersEstimation.clusterLagDistributionId] = stream_settings['parameter_estimation_folder']+ClusteringParametersEstimation.clusterLagDistributionId
        self.stream_settings = stream_settings
        self.hdsClustering = HDStreaminClustering(**self.stream_settings)
    def run(self, iterator): self.hdsClustering.cluster(iterator)
    @staticmethod
    def emptyClusterFilteringMethod(hdStreamClusteringObject, currentMessageTime): pass
    @staticmethod
    def clusterLagDistributionMethod(hdStreamClusteringObject, currentMessageTime):
        lagDistribution = defaultdict(int)
        for cluster in hdStreamClusteringObject.clusters.values():
            lag=DateTimeAirthematic.getDifferenceInTimeUnits(currentMessageTime, cluster.lastStreamAddedTime, hdStreamClusteringObject.stream_settings['time_unit_in_seconds'].seconds)
            lagDistribution[str(lag)]+=1
        print currentMessageTime, len(hdStreamClusteringObject.clusters)
        iterationData = {
                         'time_stamp': getStringRepresentationForTweetTimestamp(currentMessageTime),
                         'settings': pprint.pformat(hdStreamClusteringObject.stream_settings),
                         ClusteringParametersEstimation.clusterLagDistributionId: lagDistribution,
                         'lag_between_streams_added_to_cluster': hdStreamClusteringObject.stream_settings['lag_between_streams_added_to_cluster']
                         }
#        print hdStreamClusteringObject.stream_settings['lag_between_streams_added_to_cluster']
        FileIO.writeToFileAsJson(iterationData, hdStreamClusteringObject.stream_settings['%s_file'%ClusteringParametersEstimation.clusterLagDistributionId])
    def plotCDFClustersLagDistribution(self, returnAxisValuesOnly=True):
        '''
        This determines the time after which a cluster can be considered 
        decayed and hence removed.
        
        Experts stream [ 0.66002386  0.07035227] 0.1 82
        Houston stream [ 0.73800037  0.05890473] 0.1 29
        
        458 (# of time units) Experts stream [ 0.66002386  0.07035227] 0.2 15
        71 (# of time units) Houston stream [ 0.73756656  0.05883258] 0.2 3
        
        '''
        def calculateInActivityTimeFor(params, probabilityOfInactivity): return int(CurveFit.inverseOfIncreasingExponentialFunction(params, 1-probabilityOfInactivity))
        data = list(FileIO.iterateJsonFromFile(self.hdsClustering.stream_settings['%s_file'%ClusteringParametersEstimation.clusterLagDistributionId]))[-1]
        total = float(sum(data['lag_between_streams_added_to_cluster'].values()))
        x = sorted(map(int, data['lag_between_streams_added_to_cluster'].keys()))
        y = getCumulativeDistribution([data['lag_between_streams_added_to_cluster'][str(i)]/total for i in x])
        exponentialCurveParams = CurveFit.getParamsAfterFittingData(x, y, CurveFit.increasingExponentialFunction, [1., 1.])
        print self.stream_settings['plot_label'], exponentialCurveParams, calculateInActivityTimeFor(exponentialCurveParams, 0.2) 
        plt.plot(x,y, 'o', label=getLatexForString(self.stream_settings['plot_label'])+getLatexForString(' (%0.2fx^{%0.2f})')%(exponentialCurveParams[0], exponentialCurveParams[1]), color=self.stream_settings['plot_color'])
        plt.plot(x,CurveFit.getYValues(CurveFit.increasingExponentialFunction, exponentialCurveParams, x), color=self.stream_settings['plot_color'], lw=2)
        plt.ylabel(r'$P\ (\ lag\ \leq\ TU\ )$'), plt.xlabel(getLatexForString(xlabelTimeUnits)), plt.title(getLatexForString('CDF for clusters lag distribution.'))
        plt.ylim((0, 1.2))
        plt.legend(loc=4)
        if returnAxisValuesOnly: plt.show()
    def plotPercentageOfClustersWithinALag(self, returnAxisValuesOnly=True):
        '''
        458 Experts stream [ 0.01860266  0.70639136] 15 0.874004297177
        80 Houston stream [ 0.0793181   0.47644004] 3 0.866127308876
        '''
        def calculatePercentageOfDecayedPhrasesFor(params, timeUnit): return 1- CurveFit.increasingExponentialFunction(params, timeUnit)
        dataDistribution = {}
        currentTimeUnit = 0
#        file='/mnt/chevron/kykamath/data/twitter/lsh_crowds/houston_stream/parameter_estimation/cluster_lag_distribution'
        file = self.hdsClustering.stream_settings['%s_file'%ClusteringParametersEstimation.clusterLagDistributionId]
        lines = list(FileIO.iterateJsonFromFile(file))
        numberOfTimeUnits = len(lines)
        for data in lines:
            totalClusters=float(sum(data[ClusteringParametersEstimation.clusterLagDistributionId].values()))
            tempArray = []
            for k, v in data[ClusteringParametersEstimation.clusterLagDistributionId].iteritems():
                k=int(k)
                if k not in dataDistribution: dataDistribution[k]=[0]*numberOfTimeUnits
                dataDistribution[k][currentTimeUnit] = v/totalClusters
                tempArray.append(v/totalClusters)
            currentTimeUnit+=1
        x = sorted(dataDistribution)
        print numberOfTimeUnits,
        y = getCumulativeDistribution([np.mean(dataDistribution[k]) for k in x])
        params = CurveFit.getParamsAfterFittingData(x, y, CurveFit.increasingExponentialFunction, [1.,1.])
        print self.stream_settings['plot_label'], params, 
        def subPlot(id, timeUnit):
            plt.subplot(id)
            print timeUnit, calculatePercentageOfDecayedPhrasesFor(params, timeUnit)
            plt.plot(x,y, 'o', label=getLatexForString(self.stream_settings['plot_label'])+getLatexForString(' (%0.2fx^{%0.2f})')%(params[0], params[1]), color=self.stream_settings['plot_color'])
            plt.plot(x, CurveFit.getYValues(CurveFit.increasingExponentialFunction, params, x), color=self.stream_settings['plot_color'], lw=2)
        if self.stream_settings['stream_id']=='experts_twitter_stream': subPlot(111, 15); plt.title(getLatexForString('Percentage of clusters within a lag'))
        else: subPlot(111, 3); plt.xlabel(getLatexForString(xlabelTimeUnits))
        plt.ylabel(r'$\%\ of\ clusters\ with\ lag\ \leq\ TU$')
        plt.legend(loc=4)
        if returnAxisValuesOnly: plt.show()
    @staticmethod
    def thresholdForDocumentToBeInCluterEstimation():
        ''' Estimate thresold for the clusters by varying the threshold_for_document_to_be_in_cluster value.
        Run this on a document set of size 100K. 
        '''
        length = 10**3
        for t in range(1, 16): 
            experts_twitter_stream_settings['threshold_for_document_to_be_in_cluster'] = t*0.05
#            print experts_twitter_stream_settings['threshold_for_document_to_be_in_cluster']
            print KMeansTweetsFile(length, **experts_twitter_stream_settings).generateStatsForStreamingLSHClustering()
        

'''    Experiments of Twitter streams starts here.    '''
experts_twitter_stream_settings['convert_data_to_message_method']=houston_twitter_stream_settings['convert_data_to_message_method']=TwitterCrowdsSpecificMethods.convertTweetJSONToMessage

def dimensionsEstimation():
#    ParameterEstimation(**experts_stream_settings).run(TwitterIterators.iterateTweetsFromExperts(), ParameterEstimation.dimensionsEstimation)
#    ParameterEstimation(**houston_stream_settings).run(TwitterIterators.iterateTweetsFromHouston(), ParameterEstimation.dimensionsEstimation)
#    ParameterEstimation.plotMethods([ParameterEstimation(**experts_twitter_stream_settings).plotGrowthOfPhrasesInTime, ParameterEstimation(**houston_twitter_stream_settings).plotGrowthOfPhrasesInTime])
    ParameterEstimation.plotMethods([ParameterEstimation(**experts_twitter_stream_settings).plotDimensionsEstimation, ParameterEstimation(**houston_twitter_stream_settings).plotDimensionsEstimation])

def dimensionsUpdateFrequencyEstimation():
#    ParameterEstimation(**experts_twitter_stream_settings).run(TwitterIterators.iterateTweetsFromExperts(), ParameterEstimation.dimensionsUpdateFrequencyEstimation)
#    ParameterEstimation(**houston_twitter_stream_settings).run(TwitterIterators.iterateTweetsFromHouston(), ParameterEstimation.dimensionsUpdateFrequencyEstimation)
    ParameterEstimation.plotMethods([ParameterEstimation(**experts_twitter_stream_settings).plotDimensionsUpdateFrequencyEstimation, ParameterEstimation(**houston_twitter_stream_settings).plotDimensionsUpdateFrequencyEstimation])

def dimensionInActivityEstimation():
    def parameterSpecificDataCollectionMethod(estimationObject, message):
        for phrase in message.vector:
            if phrase in estimationObject.phraseTextToPhraseObjectMap:
                phraseObject = estimationObject.phraseTextToPhraseObjectMap[phrase]
                lag=DateTimeAirthematic.getDifferenceInTimeUnits(message.timeStamp, phraseObject.latestOccuranceTime, estimationObject.twitter_stream_settings['time_unit_in_seconds'].seconds)
                estimationObject.lagBetweenMessagesDistribution[str(lag)]+=1
#    ParameterEstimation(**experts_twitter_stream_settings).run(TwitterIterators.iterateTweetsFromExperts(), ParameterEstimation.dimensionInActivityTimeEstimation, parameterSpecificDataCollectionMethod)
#    ParameterEstimation(**houston_twitter_stream_settings).run(TwitterIterators.iterateTweetsFromHouston(), ParameterEstimation.dimensionInActivityTimeEstimation, parameterSpecificDataCollectionMethod)
#    ParameterEstimation.plotMethods([ParameterEstimation(**experts_twitter_stream_settings).plotCDFDimensionsLagDistribution, ParameterEstimation(**houston_twitter_stream_settings).plotCDFDimensionsLagDistribution])
    ParameterEstimation.plotMethods([ParameterEstimation(**experts_twitter_stream_settings).plotPercentageOfDimensionsWithinALag, ParameterEstimation(**houston_twitter_stream_settings).plotPercentageOfDimensionsWithinALag])

experts_twitter_stream_settings['cluster_filtering_method']=houston_twitter_stream_settings['cluster_filtering_method']=ClusteringParametersEstimation.emptyClusterFilteringMethod
def clusterDecayEstimation():
    def analyzeClusterLag(streamCluster, stream, **stream_settings):
        lag=DateTimeAirthematic.getDifferenceInTimeUnits(streamCluster.lastStreamAddedTime, stream.lastMessageTime, stream_settings['time_unit_in_seconds'].seconds)
        stream_settings['lag_between_streams_added_to_cluster'][str(lag)]+=1
    experts_twitter_stream_settings['cluster_analysis_method'] = houston_twitter_stream_settings['cluster_analysis_method'] = ClusteringParametersEstimation.clusterLagDistributionMethod
    experts_twitter_stream_settings['lag_between_streams'] = houston_twitter_stream_settings['lag_between_streams'] = analyzeClusterLag
    experts_twitter_stream_settings['lag_between_streams_added_to_cluster']=houston_twitter_stream_settings['lag_between_streams_added_to_cluster']=defaultdict(int)
#    ClusteringParametersEstimation(**experts_twitter_stream_settings).run(TwitterIterators.iterateTweetsFromExperts())
#    ClusteringParametersEstimation(**houston_twitter_stream_settings).run(TwitterIterators.iterateTweetsFromHouston())
#    ParameterEstimation.plotMethods([ClusteringParametersEstimation(**experts_twitter_stream_settings).plotCDFClustersLagDistribution, ClusteringParametersEstimation(**houston_twitter_stream_settings).plotCDFClustersLagDistribution])
    ParameterEstimation.plotMethods([ClusteringParametersEstimation(**experts_twitter_stream_settings).plotPercentageOfClustersWithinALag, ClusteringParametersEstimation(**houston_twitter_stream_settings).plotPercentageOfClustersWithinALag])

def thresholdForDocumentToBeInCluterEstimation():
    ClusteringParametersEstimation.thresholdForDocumentToBeInCluterEstimation()

if __name__ == '__main__':
#    dimensionsEstimation()
#    dimensionsUpdateFrequencyEstimation()
#    dimensionInActivityEstimation()
#    clusterDecayEstimation()
    thresholdForDocumentToBeInCluterEstimation()
    