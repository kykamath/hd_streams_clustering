'''
Created on Jun 30, 2011

@author: kykamath
'''
import sys
sys.path.append('../')
from pymongo import Connection
from datetime import datetime, timedelta
from library.file_io import FileIO
from library.twitter import getStringRepresentationForTweetTimestamp

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
        for tweet in tweets.find({'ca': {'$gt':currentDay, '$lt': currentDay+timedelta(seconds=86399)}}, limit=1000, fields=['ca', 'tx', 'uid']):
            screenName = GenerateData.getScreenName(tweet['uid'])
            if screenName!=None: 
                data = {'id': tweet['_id'], 'text': tweet['tx'], 'created_at':getStringRepresentationForTweetTimestamp(tweet['ca']), 'user':{'screen_name': GenerateData.getScreenName(tweet['uid'])}}
                FileIO.writeToFileAsJson(data, fileName) 

if __name__ == '__main__':
    GenerateData.writeTweetsForDay(datetime(2010,12,1))
