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
import datetime
from dateutil import tz
from mysolr import Solr
#
from pymongo import Connection
import gridfs
import bson
from twisted.internet import task

import argparse

log.startLogging(sys.stdout)

MAX_WAIT = 180.0 # interval time between each commit
MAX_LINE = 500 # number of lines after which we have to commit
LINE_CONS = 0    # number of lines consumed

class LogConsumer():

    def __init__(self, datadir, znode_path, zcrq, solr, mongodb = "resilience10"
                 , normalizer='/home/lahoucine/src/pylogsparser/normalizers'
                 , mongoAddr = "localhost", mongoPort= 28017):
        self.datadir = datadir
        self.znode_path = znode_path
        self.zcrq = zcrq
        self.ln = lognormalizer.LogNormalizer(normalizer)
        self.solr = solr
        self._init_mongo(mongodb, mongoAddr, mongoPort)
        self.timer = task.LoopingCall(self._solrCommit)
        self.consumed = 0
        
    def _solrCommit(self, stop=True):
        print "commit in solr"
        self.consumed = 0
        if stop:
            self.timer.stop()
        else:
            self.timer.reset()
        self.solr.commit()
        
    
    def _init_mongo(self,dbName = "resilience", mongoAddr ="localhost", mongoPort = 28017):
        connection = Connection(mongoAddr,mongoPort)
        self.db = connection[dbName]
        self.mongofs = gridfs.GridFS(self.db)
            
    def consume_many(self):
        def _consume():
            self.consume()
            reactor.callLater(1, _consume)
            
        reactor.callLater(1, _consume)
    
    def consume(self):
        def _consuming(item):
            log.msg('Consuming %s' % item.data)
            try:
                if not self.timer.running:
                    self.timer.start(MAX_WAIT, False)
                
                self._gridfs_consum(item)    
                log.msg("Indexed in solr: %s" % item.data)   
                log.msg('Remove %s from log chunk path.' % item.data)
                item.delete()
            except Exception, e:
                log.msg('WARNING unable to suppress %s due to : %s' % (item.data, e))
        d = self.zcrq.get()
        d.addCallback(_consuming)
            
    def _local_consum(self,item):
        file = open(item.data, 'r')
        for line in file:
            logLine = {'raw':line.rstrip('\r\n')}
            print "indexing:", line
            self.ln.lognormalize(logLine) 
            self.index(logLine)
            self.consumed += 1
            self._check_consumed()

    def _check_consumed(self):
        self.consumed += 1
        print "consumed", self.consumed
        if self.consumed == MAX_LINE:
            print "conseumed reached",self.consumed
            self._solrCommit(False)
    
    def _gridfs_consum(self,item):
        file = self.mongofs.get(bson.ObjectId(item.data))
        line = file.readline()
        while line:
            logLine = {'raw':line.rstrip('\r\n')}
            print "indexing:", line
            self.ln.lognormalize(logLine) 
            logLine["fileid"] = item.data
            self.index(logLine)
            line = file.readline()
            self._check_consumed()

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
        

def cb_connected(useless, zc, datadir, solr, mongoAddr, mongoPort, normalizer):
    
    def _err(error):
        log.msg('Queue znode seems to already exists : %s' %error)
    znode_path = '/log_chunk_produced31'
    zcrq = ReliableQueue(znode_path, zc, persistent = True)
    #d.addCallback(lambda x: log.msg('Queue znode created at %s' % znode_path))
    #d.addErrback(_err)
    lc = LogConsumer(datadir, znode_path, zcrq, solr, "resilience10", normalizer, mongoAddr, mongoPort)
    lc.consume_many()
    #lc.consume()
    

def main():
    
    params = sys.argv[1:]
        
    parser = argparse.ArgumentParser(description='A log consumer')
    parser.add_argument('-z','--zkServer',help='address of the zookeeper server', 
                                          default="localhost:2181", required=True)
    parser.add_argument('-m','--mongoAddr',help='address of the mongodb server', 
                                          default="localhost", required=True)
    parser.add_argument('-p','--mongoPort',help='port of the mongodb server', type=int, 
                                          default="28017", required=True)
    parser.add_argument('-s','--solrAddr',help='address of the solr server', 
                                          default="http://localhost:8983/solr/collection1/", required=True)
    parser.add_argument('-n','--normalizer',help=' path to pylogs parser normalizers', 
                                          default="/home/lahoucine/src/pylogsparser/normalizers", required=True)
    
    args = parser.parse_args(params)
    
    datadir = '/tmp/rawdata'
    mongodb = 'resilience10'
    zkAddr = args.zkServer
    mongoAddr = args.mongoAddr
    mongoPort = args.mongoPort
    solrAddr = args.solrAddr
    normalizer = args.normalizer

    # if not os.path.isdir(datadir):
    #    os.mkdir(datadir) 
    
    sol = Solr(solrAddr)
    zc = RetryClient(ZookeeperClient(zkAddr))
    d = zc.connect()
    d.addCallback(cb_connected, zc, datadir, sol, mongoAddr, mongoPort, normalizer)
    d.addErrback(log.msg)
    reactor.run()
    
if __name__ == "__main__":
    main()
