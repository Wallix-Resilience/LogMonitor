#!/usr/bin/python
# coding=utf-8
import sys
import os
import random
import time
from twisted.python import log
from twisted.internet import reactor
from txzookeeper.client import ZookeeperClient
from txzookeeper.retry import RetryClient
from txzookeeper.queue import ReliableQueue
from logsparser import lognormalizer
from resilience.zookeeper.configure.config import Config
from twisted.internet import task
import argparse
from  pymongo.errors import AutoReconnect
from datetime import datetime, timedelta, tzinfo
from resilience.data.DataStorage import MongoGridFs
from resilience.data.DataIndexer import SolrIndexer

log.startLogging(open('./consumer', 'w'))

MAX_WAIT = 60.0 # time interval between each commit
MAX_LINE = 30000 # number of lines after which we have to commit
LINE_CONS = 0    # number of lines consumed

# Solr hates unicode control chars...
UNICODE_CONTROL_CHARACTERS = {}
for ordinal in range(0x20):
    if chr(ordinal) not in '\t\r\n':
        UNICODE_CONTROL_CHARACTERS[ordinal] = None
        
class LogConsumer():
    """
    This class represent a Consumer. Its role is to retrieve tasks from
    the queue for consuming. Each task contain a file id.
    
    MAX_WAIT: time interval between each commit
    MAX_LINE: number of lines after which we have to commit
    LINE_CONS: number of lines consumed
    """
    
    def __init__(self, znode_path, zk, zcrq, storage, indexer, normalizer = ""):
        self.znode_path = znode_path
        self.zcrq = zcrq
        self.zk = zk
        self.ln = lognormalizer.LogNormalizer(normalizer)
        self.conf = Config(zk)
        self.storage = storage
        self.indexer = indexer
        self.timer = task.LoopingCall(self._indexCommiter)
        self.consumed = 0
             
        
    def consume(self):
        """
        get item from the Reliable Queue and start consuming
        """
        def _requeue(item):
            """
            requeue an item in the Reliable Queue if consuming fails
            """
            print "delete ", item.path + "-processing"
            try:
                self.zk.delete(item.path + "-processing")
            except:
                log.msg("Can't requeue item %s" % item.data)
            
        def _consumItem(item):
            """
            start consuming: open the file referenced by the task
            and Initialize a timer for commits in solr.
            """
            log.msg('Consuming %s' % item.data)
            try:
                if not self.timer.running:
                    self.timer.start(MAX_WAIT, False)
                print "path", item.path                
                ok = self._consumItem(item)
                if  ok:    
                    log.msg("Indexed in solr: %s" % item.data)   
                    log.msg('Remove %s from log chunk path.' % item.data)
                    item.delete()
                else:
                    #if not OK requeue
                    log.msg("Not totaly indexed: %s" % item.data)
                    _requeue(item)
                    log.msg("Requeue of item: %s" % item.data)
            except Exception, e:
                log.msg('WARNING unable to suppress %s due to : %s' % (item.data, e))
                
            d = self.zcrq.get()
            d.addCallback(_consuming)
            
        d = self.zcrq.get()
        d.addCallback(_consuming)
        
    def _check_consumed(self):
        """
        verify if consuming limit is reached and then commit in Solr
        """
        self.consumed += 1
        print "consumed", self.consumed
        if self.consumed == MAX_LINE:
            print "consumption limit reached",self.consumed
            self._indexCommiter(False)
            
    def _indexCommiter(self, stop=True):
        """
        Make a commit in Solr and manage the commit timer
        @param stop: if true, the timer will be stopped else it will be reset
        """
        self.consumed = 0
        #stop the timer if is just an initialization 
        if stop:
            self.timer.stop()
        else:
            self.timer.reset()
        self.indexer.commit()                   
        
    def _consumItem(self,item):
        """
        consume a file from GridFs: parsing + indexing
        @param item: represent the id of the file to consume in GridFS
        @return: true if consuming in GridFS succeed
        """                    
        try:
            fileItem = self.storage.getFile(item)
            line = fileItem.readline()
            # verifier si zookeeper est présent pour ne pas continuer de consomer
            # un fichier dans mongo qui est re-integre a la "queue"
            # while line and zookeeper_ok 
            while line:
                logLine = {'raw':line.rstrip('\r\n')}
                print "indexing:", line
                self.ln.lognormalize(logLine) 
                logLine["fileid"] = item.data
                ret = self.indexer.index(logLine)
                if not ret:
                    return False
                #save current position after indexing succed
                self.storage.saveCurrentPosition(fileItem)
                line = fileItem.readline()
                self._check_consumed()
        except Exception, e:
            print e
            return False
        return True


def cb_connected(useless, zc, normalizer):
    """
    Access to a Reliable Queue and start an instance of consumer
    @zc: Zookeeper client instance
    @normalizer: PylogsParser normalizer instance
    """  
    def _err(error):
        log.msg('Queue znode seems to already exists : %s' %error)
    znode_path = '/log_chunk_produced42'
    zcrq = ReliableQueue(znode_path, zc, persistent = True)
    #d.addCallback(lambda x: log.msg('Queue znode created at %s' % znode_path))
    #d.addErrback(_err)
        
    storage = MongoGridFs("resilience21", Config(zc), reactor)
    indexer = SolrIndexer(Config(zc))
    
    lc = LogConsumer(znode_path, zc, zcrq, storage, indexer, normalizer)
    lc.consume()


def main():
    """
    bootstrap function
    """
    params = sys.argv[1:]
    parser = argparse.ArgumentParser(description='A log consumer')
    parser.add_argument('-z','--zkServer',help='address of the zookeeper server', 
                                          default="fd88:9fde:bd6e:f57a:0:1d7b:9dea:802:29017", required=True)
    parser.add_argument('-n','--normalizer',help=' path to pylogs parser normalizers', 
                                          default="/home/lahoucine/src/pylogsparser/normalizers", required=True)
    
    args = parser.parse_args(params)
    
    zkAddr = args.zkServer
    normalizer = args.normalizer
    try:
        zc = RetryClient(ZookeeperClient(zkAddr))
        d = zc.connect()
        d.addCallback(cb_connected, zc, normalizer)
        d.addErrback(log.msg)
    except:
        print "Can't connect to Zookeeper"
        return
    
    reactor.run()
    
if __name__ == "__main__":
    main()
