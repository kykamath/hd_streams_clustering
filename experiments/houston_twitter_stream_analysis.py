'''
Created on Jun 30, 2011

@author: kykamath
'''
import sys, os
sys.path.append('../')
from pymongo import Connection
from twitter_streams_clustering import TwitterIterators,\
    TwitterCrowdsSpecificMethods
from datetime import datetime, timedelta
from library.file_io import FileIO
from library.twitter import getStringRepresentationForTweetTimestamp
from settings import twitter_stream_settings

mongodb_connection = Connection('sarge', 27017)
tweets = mongodb_connection.old_hou.Tweet
users = mongodb_connection.old_hou.User

houston_data_folder = '/mnt/chevron/kykamath/data/twitter/houston/'
        
class GenerateData:
    userIdToScreenNameMap = {}
    @staticmethod
    def getScreenName(uid):
        if uid not in GenerateData.userIdToScreenNameMap: 
            userObject = users.find_one({'_id': uid}, fields=['sn'])
            if userObject!=None: GenerateData.userIdToScreenNameMap[uid]=userObject['sn']
        return GenerateData.userIdToScreenNameMap.get(uid, None)
    @staticmethod
    def writeTweetsForDay(currentDay):
        fileName = houston_data_folder+FileIO.getFileByDay(currentDay)
        for tweet in tweets.find({'ca': {'$gt':currentDay, '$lt': currentDay+timedelta(seconds=86399)}}, fields=['ca', 'tx', 'uid']):
            screenName = GenerateData.getScreenName(tweet['uid'])
            if screenName!=None: 
                data = {'id': tweet['_id'], 'text': tweet['tx'], 'created_at':getStringRepresentationForTweetTimestamp(tweet['ca']), 'user':{'screen_name': GenerateData.getScreenName(tweet['uid'])}}
                FileIO.writeToFileAsJson(data, fileName) 
        os.system('gzip %s'%fileName)
    @staticmethod
    def generateHoustonData():
        currentDay = datetime(2010,1,23)
        endingDay = datetime(2011,5,31)
        while currentDay<=endingDay:
            print 'Generating data for: ', currentDay
            GenerateData.writeTweetsForDay(currentDay)
            currentDay+=timedelta(days=1)
        
if __name__ == '__main__':
    GenerateData.generateHoustonData()
