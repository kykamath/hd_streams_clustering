'''
Created on Jun 12, 2011

@author: kykamath
'''

class DistributedMapException(Exception):
    def __init__(self, error='Distributed map exception'):
        self.message = error

class DistributedMemcacheMap:
    '''
    A distributed hashmap that uses memcached.
    '''
    try:
        import memcache
    except ImportError: print 'Install python-memcache to use DistributedMemcachedMap'
    global memcache
    def __init__(self, server, port=11211):
        self.cache = memcache.Client(['%s:%s'%(server, port)], debug=0)
        if self.cache.set('server_running', 1)==0: 
            raise DistributedMapException('Memcached server not running at %s:%s'%(server, port))

if __name__ == '__main__':
    DistributedMemcacheMap('localhost')