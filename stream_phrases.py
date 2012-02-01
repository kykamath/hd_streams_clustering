'''
Created on Feb 1, 2012

@author: kykamath
'''
from library.twitter import TweetFiles
from twitter_streams_clustering import TwitterCrowdsSpecificMethods
from operator import itemgetter
from library.classes import GeneralMethods

settings = dict(
            min_phrase_length=2,
            max_phrase_length=3
            )


INTERVAL_LENGTH_IN_SECONDS = 60*30
NO_OF_TOP_PHRASES = 10

OVERALL_COUNT = 'overall_count'
INTERVALWISE_COUNT = 'intervalwise_count'

class Phrase:
    def __init__(self, id, timeStamp): self.id, self.lastUpdatedTime, self.score = id, timeStamp, 1.0
    
class ScoringAlgorithm(object):
    def __init__(self, id=OVERALL_COUNT): 
        self.id = id
        self.phrases = {}
    def updateScore(self, phraseId, phraseOccurrenceTime, *args, **kwargs): 
        if phraseId not in self.phrases: self.phrases[phraseId] = Phrase(phraseId, phraseOccurrenceTime)
        else: self.phrases[phraseId].score+=1
    def getTopPhrases(self):
        return sorted(self.phrases.iteritems(), key=lambda (_, phrase): phrase.score, reverse=True)[:NO_OF_TOP_PHRASES]

class IntervalwiseCountAlgorithm(ScoringAlgorithm):
    def __init__(self, **kwargs): 
        super(IntervalwiseCountAlgorithm, self).__init__(INTERVALWISE_COUNT, **kwargs)
        
def iteratePhrases():
    for tweet in TweetFiles.iterateTweetsFromGzip('/mnt/chevron/kykamath/data/twitter/tweets_by_trends/2011_2_6.gz'):
        message = TwitterCrowdsSpecificMethods.convertTweetJSONToMessage(tweet, **settings)
        if message.vector:
            for phrase in message.vector: 
                if phrase!='': yield (phrase, GeneralMethods.approximateEpoch(GeneralMethods.getEpochFromDateTimeObject(message.timeStamp), 60))

    
algorithm = ScoringAlgorithm()
for phraseCount, (phraseId, phraseOccurrenceTime) in enumerate(iteratePhrases()):
    algorithm.updateScore(phraseId, phraseOccurrenceTime)
    print phraseOccurrenceTime
    if phraseCount >= 5000: break
    
topPhrases = algorithm.getTopPhrases()
for k, v in topPhrases:
    print k, v.score