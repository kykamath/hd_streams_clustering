'''
Created on Jun 30, 2011

@author: kykamath
'''
from pymongo import Connection
class GenerateData:
    @staticmethod
    def generateHoustonFiles():
        mongodb_connection = Connection('sarge', 27017)
        tweets = mongodb_connection.old_hou.Tweet
        users = mongodb_connection.old_hou.User
        for tweet in tweets.find(limit=10):
            print tweet

if __name__ == '__main__':
    GenerateData.generateHoustonFiles()