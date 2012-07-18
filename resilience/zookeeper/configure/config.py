#!/usr/bin/python

from txzookeeper.client import ZookeeperClient
from txzookeeper.retry import RetryClient



class Config():
        
    def __init__(self, zkAddr):
        self.zk = RetryClient(ZookeeperClient(zkAddr))
        
    
    def add_mongod(self, mongod):
        self.zk.create("/monggodb/mongod/")
        
    
    def add_solr(self, solr):
        self.zk.create("/solr/")
    
    def get_mongod(self, callback=None):

        if callback == None:
            #get children
            pass
        else:
            def _call(self):
                pass
            #get children and watch
            pass
                   
    def get_solr(self):
        pass
    
    def _get_conf(self, path, callback = None):
        
        def _get_value(m):
            return m[0]
            
        def _call(m):
            dat, m = self.zc.get_and_watch(path)
            val = dat.addCallback(_get_value)
            callback(val)
            m.addCallback(_call)
        
        if not callback:
            data = self.zc.get_data(path)
            data.addCallback(_get_value)
            
        else:
            data, diff = self.zc.get_and_watch(path)
            data.addCallback(_get_value)
            diff.addCallback(_call)
            
        
        
    
    
if __name__ == "__main__":
    pass
    
    

