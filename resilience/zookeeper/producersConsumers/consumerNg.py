#!/usr/bin/python

import sys
import os
import random
import time
from twisted.python import log
from twisted.internet import reactor, defer
from txzookeeper.client import ZookeeperClient
from txzookeeper.retry import RetryClient
from txzookeeper.queue import ReliableQueue
from logsparser import lognormalizer
import solr #solrpy
import urllib2
import requests
import datetime
from dateutil import tz
from mysolr import Solr
#
from pymongo import Connection
import gridfs
import bson

log.startLogging(sys.stdout)

class LogConsumer():

    def __init__(self, datadir, znode_path, zcrq, solr, normalizer='/home/lahoucine/src/pylogsparser/normalizers'):
        self.datadir = datadir
        self.znode_path = znode_path
        self.zcrq = zcrq
        self.p = 0
        self.ln = lognormalizer.LogNormalizer(normalizer)
        self.solr = solr
        self._init_mongo("resilience2")

    def _init_mongo(self,dbName = "resilience"):
        connection = Connection()
        db = connection[dbName]
        self.mongofs = gridfs.GridFS(db)
            
    def consume_many(self):
        def _consume():
            self.consume()
            self.p += 1
            if self.p < 1000000:
                reactor.callLater(1, _consume)
        reactor.callLater(1, _consume)
    
    def _local_consum(self,item):
        file = open(item.data, 'r')
        for line in file:
            logLine = {'raw':line.rstrip('\r\n')}
            print "indexing:", line
            self.ln.lognormalize(logLine) 
            self.index(logLine)
            

    
    def _gridfs_consum(self,item):
        file = self.mongofs.get(bson.ObjectId(item.data))
        line = file.readline()
        while line:
            logLine = {'raw':line.rstrip('\r\n')}
            print "indexing:", line
            self.ln.lognormalize(logLine) 
            self.index(logLine)
            line = file.readline()
                    
           
    
    def consume(self):
        def _consuming(item):
            log.msg('Consuming %s' % item.data)
            try:
                self._gridfs_consum(item)
                self.solr.commit()
                log.msg("Indexed in solr: %s" % item.data)   
                log.msg('Remove %s from log chunk path.' % item.data)
                item.delete()
            except Exception, e:
                log.msg('WARNING unable to suppress %s due to : %s' % (item.data, e))
        d = self.zcrq.get()
        d.addCallback(_consuming)

    def index(self,data):
        for key, value in data.items():
            if isinstance(value,datetime.datetime):
                try:
                    value = solr.core.utc_to_string(value)
                except:
                    pst = tz.gettz('Europe/Paris')
                    value = value.replace(tzinfo=pst)
                    value = solr.core.utc_to_string(value)
                    data[key] = value   
        try:
            print "json:", data
            self.solr.update([data],commit=False)
            
        except Exception, e:
            log.msg("WARNING unable to index %s due to : %s" % (data,e))
        

def cb_connected(useless, zc, datadir,solr):
    def _err(error):
        log.msg('Queue znode seems to already exists : %s' %error)
    znode_path = '/log_chunk_produced18'
    zcrq = ReliableQueue(znode_path, zc, persistent = True)
    d.addCallback(lambda x: log.msg('Queue znode created at %s' % znode_path))
    d.addErrback(_err)
    lc = LogConsumer(datadir, znode_path, zcrq, solr)
    lc.consume_many()
    #lc.consume()
    
if __name__ == "__main__":
    datadir = '/tmp/rawdata'
    if not os.path.isdir(datadir):
        os.mkdir(datadir) 
   # sol = Solr('http://localhost:8983/solr/core1')
    sol = Solr('http://localhost:8983/solr/collection1/')
    zc = RetryClient(ZookeeperClient("127.0.0.1:2181"))
    d = zc.connect()
    d.addCallback(cb_connected, zc, datadir,sol)
    d.addErrback(log.msg)
    reactor.run()
