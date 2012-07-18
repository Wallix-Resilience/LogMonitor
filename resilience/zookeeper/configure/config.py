#!/usr/bin/python

from txzookeeper.client import ZookeeperClient
from txzookeeper.retry import RetryClient
import sys

from twisted.python import log
from twisted.internet import reactor, defer

log.startLogging(sys.stdout)


class Config():
        
    def __init__(self, zkAddr):
        self.zk = zkAddr
        
    
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
            value = m[0]
            if callback:
                callback(value)
            print  value
            
        def _call(m):
            print "func call"
            dat, m = self.zk.get_and_watch(path)
            dat.addCallback(_get_value)
            print "get res", dat.returnValue()
            m.addCallback(_call)
        
        if not callback:
            data = self.zk.get(path)
            data.addCallback(_get_value)
            
        else:
            data, diff = self.zk.get_and_watch(path)
            data.addCallback(_get_value)
            diff.addCallback(_call)
            
        
        
    
    
if __name__ == "__main__":
    def cb_connected(self, zc):
        cfg = Config(zc)
        def _call(m):
            print "call",m
        res = cfg._get_conf("/test",_call)
        print "res", res
        
        
    zc = RetryClient(ZookeeperClient("127.0.0.1:2181"))
    d = zc.connect()
    d.addCallback(cb_connected, zc)
    d.addErrback(log.msg)
    reactor.run()

    
    

