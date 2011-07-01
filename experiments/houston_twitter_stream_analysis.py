'''
Created on Jun 30, 2011

@author: kykamath
'''
from pymongo import Connection

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
#    @staticmethod
#    def generateHoustonFiles():
#        if tweet['uid'] not in userIdToScreenNameMap: 
#            userObject = tweets.find_one({'_id': tweet['uid']})
#            if userObject!=None: user
#        userIdToScreenNameMap = {}
#        for tweet in tweets.find(limit=10):
#            print tweet['ca'], tweet['tx'], tweet['uid']
#            if tweet['uid'] not in userIdToScreenNameMap: 
#                userObject = tweets.find_one({'_id': tweet['uid']})
#                if userObject!=None: user

if __name__ == '__main__':
#    GenerateData.generateHoustonFiles()
    print GenerateData.userIdToScreenNameMap
    print GenerateData.getScreenName(5841)
    print GenerateData.userIdToScreenNameMap