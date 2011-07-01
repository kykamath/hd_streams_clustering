'''
Created on Jun 30, 2011

@author: kykamath
'''
from pymongo import Connection
from datetime import datetime, timedelta

mongodb_connection = Connection('sarge', 27017)
tweets = mongodb_connection.old_hou.Tweet
users = mongodb_connection.old_hou.User
        
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
        for tweet in tweets.find({'ca': {'$gt':currentDay, '$lt': currentDay+timedelta(seconds=86399)}}, limit=10, fields=['ca', 'tx', 'uid']):
            screenName = GenerateData.getScreenName(tweet['uid'])
            if screenName!=None: print tweet['ca'], tweet['tx'], GenerateData.getScreenName(tweet['uid'])
            
#    @staticmethod
#    def generateHoustonFiles():
#        for tweet in tweets.find(limit=10, fields=['ca', 'tx', 'uid']):
#            screenName = GenerateData.getScreenName(tweet['uid'])
#            if screenName!=None: print tweet['ca'], tweet['tx'], GenerateData.getScreenName(tweet['uid'])

if __name__ == '__main__':
    GenerateData.writeTweetsForDay(datetime(2010,12,1))
#    print GenerateData.userIdToScreenNameMap
#    print GenerateData.getScreenName(5841)
#    print GenerateData.userIdToScreenNameMap