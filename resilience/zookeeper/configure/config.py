#!/usr/bin/python

from txzookeeper.client import ZookeeperClient
from txzookeeper.retry import RetryClient
import zookeeper
import sys

from twisted.python import log
from twisted.internet import reactor, defer

log.startLogging(sys.stdout)


class Config():
        
    def __init__(self, zkAddr):
        self.zk = zkAddr
        
    def _err(self,error):
        log.msg('node seems to already exists : %s' % error)
        
    def add_mongod(self, mongod):
        d = self.zk.create("/monggodb/mongod/%s" % str(mongod))
        d.addErrback(self._err)
                        
    def add_solr(self, solr):
        d =  self.zk.create("/solr/%s" % str(solr))
        d.addErrback(self._err)
    
    def get_mongod_all(self, callback = None):
        self._get_conf("/mongo", callback)
                   
    def get_solr_all(self, callback = None):
        self._get_conf("/solr", callback)
    
    def _get_conf_all(self, path, callback = None):
        
        def _get_value(m):
            value = m
            if callback:
                callback(value)   
        def _call(m):
            dat, m = self.zk.get_children_and_watch(path)
            dat.addCallback(_get_value)
            m.addCallback(_call)
        
        if not callback:
            data = self.zk.get(path)
            data.addCallback(_get_value)
        else:
            data, diff = self.zk.get_children_and_watch(path)
            data.addCallback(_get_value)
            diff.addCallback(_call)
            
    
if __name__ == "__main__":
    def cb_connected(self, zc):
        cfg = Config(zc)
        def _call(m):
            print "call",m
        cfg._get_conf("/test",_call)
        cfg.get_solr(_call)
        cfg.get_mongod(_call)
        cfg.add_solr('localhost6:20120')
    zc = RetryClient(ZookeeperClient("127.0.0.1:2181"))
    d = zc.connect()
    d.addCallback(cb_connected, zc)
    d.addErrback(log.msg)
    reactor.run()

    
    

