'''
Created on Jun 22, 2011

@author: kykamath
'''

UNIQUE_LIBRARY_KEY = '::ilab::'

class Settings(dict):
    '''
    This class was obtained from Jeff McGee.
    https://github.com/jeffamcgee
    '''
    def __getattr__(self, name): return self[name]
    def __setattr__(self, name, value): self[name] = value
        
class GeneralMethods:
    callMethodEveryIntervalVariable=None
    @staticmethod
    def callMethodEveryInterval(method, interval, currentTime, **kwargs):
        if GeneralMethods.callMethodEveryIntervalVariable==None: GeneralMethods.callMethodEveryIntervalVariable=currentTime
        if currentTime-GeneralMethods.callMethodEveryIntervalVariable>=interval:
            method(**kwargs)
            GeneralMethods.callMethodEveryIntervalVariable=currentTime
    @staticmethod
    def reverseDict(map): 
        dictToReturn = dict([(v,k) for k,v in map.iteritems()])
        if len(dictToReturn)!=len(map): raise Exception()
        return dictToReturn
            
class TwoWayMap:
    '''
    A data strucutre that enables 2 way mapping.
    '''
    MAP_FORWARD = 1
    MAP_REVERSE = -1
    def __init__(self): self.data = {TwoWayMap.MAP_FORWARD: {}, TwoWayMap.MAP_REVERSE: {}}
    def set(self, mappingDirection, key, value): 
        if value in self.data[-1*mappingDirection]: self.remove(mappingDirection, self.data[-1*mappingDirection][value])
        self.data[mappingDirection][key]=value
        self.data[-1*mappingDirection][value]=key
    def get(self, mappingDirection, key): return self.data[mappingDirection][key]
    def remove(self, mappingDirection, key):
        if key in self.data[mappingDirection]:
            value = self.data[mappingDirection][key]
            del self.data[mappingDirection][key]; del self.data[-1*mappingDirection][value]
    def getMap(self, mappingDirection): return self.data[mappingDirection]
    def contains(self, mappingDirection, key): return  key in self.data[mappingDirection]
    def __len__(self): return len(self.data[TwoWayMap.MAP_FORWARD])