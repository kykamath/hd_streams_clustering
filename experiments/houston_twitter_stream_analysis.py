'''
Created on Jun 30, 2011

@author: kykamath
'''
import sys, os
sys.path.append('../')
from pymongo import Connection
from datetime import datetime, timedelta
from library.file_io import FileIO
from library.twitter import getStringRepresentationForTweetTimestamp

mongodb_connection = Connection('sarge', 27017)
tweets = mongodb_connection.old_hou.Tweet
users = mongodb_connection.old_hou.User

houston_data_folder = '/mnt/chevron/kykamath/data/twitter/houston/'
        
class GenerateHoustonTweetsData:
    userIdToScreenNameMap = {}
    @staticmethod
    def getScreenName(uid):
        if uid not in GenerateHoustonTweetsData.userIdToScreenNameMap: 
            userObject = users.find_one({'_id': uid}, fields=['sn'])
            if userObject!=None: GenerateHoustonTweetsData.userIdToScreenNameMap[uid]=userObject['sn']
        return GenerateHoustonTweetsData.userIdToScreenNameMap.get(uid, None)
    @staticmethod
    def writeTweetsForDay(currentDay):
        fileName = houston_data_folder+FileIO.getFileByDay(currentDay)
        for tweet in tweets.find({'ca': {'$gt':currentDay, '$lt': currentDay+timedelta(seconds=86399)}}, fields=['ca', 'tx', 'uid']):
            screenName = GenerateHoustonTweetsData.getScreenName(tweet['uid'])
            if screenName!=None: 
                data = {'id': tweet['_id'], 'text': tweet['tx'], 'created_at':getStringRepresentationForTweetTimestamp(tweet['ca']), 'user':{'screen_name': GenerateHoustonTweetsData.getScreenName(tweet['uid'])}}
                FileIO.writeToFileAsJson(data, fileName) 
        os.system('gzip %s'%fileName)
    @staticmethod
    def generateHoustonData():
        currentDay = datetime(2011,1,23)
        endingDay = datetime(2011,5,31)
        while currentDay<=endingDay:
            print 'Generating data for: ', currentDay
            GenerateHoustonTweetsData.writeTweetsForDay(currentDay)
            currentDay+=timedelta(days=1)
        
if __name__ == '__main__':
    GenerateHoustonTweetsData.generateHoustonData()
    