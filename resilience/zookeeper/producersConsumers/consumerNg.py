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
import solr #solrpy
from dateutil import tz
from mysolr import Solr
from pymongo import Connection
import gridfs
import bson
from twisted.internet import task
import argparse
from  pymongo.errors import AutoReconnect
from datetime import datetime, timedelta, tzinfo


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
    
    def __init__(self, znode_path, zk, zcrq, mongodb = "resilience21"
                 , normalizer = ""):
        self.znode_path = znode_path
        self.zcrq = zcrq
        self.zk = zk
        self.ln = lognormalizer.LogNormalizer(normalizer)
        self.solr = None
        self.mongofs = None
        self.db = None
        self.conf = Config(zk)
        self._init_solr()
        self._init_mongo(mongodb)
        self.timer = task.LoopingCall(self._solrCommit)
        self.consumed = 0
        
    
    def _init_solr(self):
        """Initialization of a Solr instance 
        """
        def _call(s):
            """Retrieve Solr configuration from Zookeeper
            and initialize a solr client             
            """
            if s:
                addr, sep, port =s[0].rpartition(":")
                #if addr.count(':') > 0:                
                #    solrAddr = "http://[%s]:%s/solr/collection1/" % (addr, port)
                #    print "addddr", solrAddr
                #else:
                solrAddr = "http://%s:%s/solr/collection1/" % (addr, port)
                print "addddr", solrAddr
                try:
                    self.solr = Solr(solrAddr)
                    log.msg("connected to solr: %s" % solrAddr )
                except Exception, e:
                    log.msg("can't connect to solr: %s" % e)
                
        self.conf.get_solr_all(_call)
        
    def _init_mongo(self,dbName):
        """Initialization of a MongoDB instance 
        """
        def _connect(mongoAdd, mongoPort, limit):
            """Connect producer to MongoDB and create a GridFs instance
            @param mongoAdd: MongoDB address 
            @param mongoPort: MongoDB port
            @param limit: initialized to zero for recursive call
            """
            try:
                connection = Connection(mongoAdd, int(mongoPort))
                self.db = connection[dbName]    
                self.mongofs = gridfs.GridFS(self.db)
                print "connected to mongodb: %s:%s" %  (mongoAdd, mongoPort)
            except AutoReconnect, e:
                print "mongodb:", e
                #time.sleep(2)
                reactor.callLater(2, _connect, mongoAdd, mongoPort, limit)#TODO: test callLater
                #to not reach python's recursion limit - 100
                #if limit < (sys.getrecursionlimit() - 100):
                #   _connect(mongoAdd, mongoPort, limit+1)
                
        def _call(m):
            """Retrieve MonogDB configuration from Zookeeper
            and call local function _connect              
            """
            if m:
                mg = m[0]
                #TODO:  catch exception, (need more than one value)
                #mongoAdd, mongoPort = mg.split(":")
                mongoAdd, sep, mongoPort =mg.rpartition(":")
                if mongoAdd == '':
                    log.msg('mongo: %s is not a correct address', mg)
                    return 
                mongoAdd = '[%s]' % mongoAdd
                print "conf mongo", mongoAdd, mongoPort
                _connect(mongoAdd, mongoPort,0)
            else:
                self.mongofs = None
                self.bd = None
                    
        self.conf.get_mongod_all(_call)
         
        
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
            
        def _consuming(item):
            """
            start consuming: open the file referenced by the task
            and Initialize a timer for commits in solr.
            """
            log.msg('Consuming %s' % item.data)
            try:
                if not self.timer.running:
                    self.timer.start(MAX_WAIT, False)
                print "path", item.path                
                ok = self._gridfs_consum(item)
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
            self._solrCommit(False)
            
    def _solrCommit(self, stop=True):
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
        try:
            self.solr.commit()
            log.msg("commit in solr")
        except:
            log.msg("can't commit in solr")
        
    def _gridfs_consum(self,item):
        """
        consume a file from GridFs: parsing + indexing
        @param item: represent the id of the file to consume in GridFS
        @return: true if consuming in GridFS succeed
        """                    
        try:
            id = bson.ObjectId(item.data)
            file = self.mongofs.get(id)
            result = self.db.fs.files.find_one({'_id':id}, {'position':1, '_id':0})
            position = result['position']
            print "POSITION:", position
            file.seek(position)
            line = file.readline()
            # verifier si zookeeper est présent pour ne pas continuer de consomer
            # un fichier dans mongo qui est re-integre a la "queue"
            # while line and zookeeper_ok 
            while line:
                logLine = {'raw':line.rstrip('\r\n')}
                print "indexing:", line
                self.ln.lognormalize(logLine) 
                logLine["fileid"] = item.data
                ret = self.index(logLine)
                if not ret:
                    return False
                #save current position after indexing succed
                self.db.fs.files.update({'_id':id},{"$set":{"position":file.tell()}})
                line = file.readline()
                self._check_consumed()
        except Exception, e:
            print e
            return False
        return True
    
    # Yoinked from wlb
    def update_wlog_for_solr(self, wlog):
        # Remove some control characters (Solr hates it)
        for key in wlog.keys():
            if isinstance(wlog[key], unicode):
                wlog[key] = wlog[key].translate(UNICODE_CONTROL_CHARACTERS)
        
        received_at = datetime.utcnow()
        if not 'date' in wlog:
            date = received_at
        else:
            date = wlog['date']
        if not 'body' in wlog:
            wlog['body'] = wlog['raw']
            
        wlog['__d_seconds'] = date.second
        wlog['__d_ms'] = date.microsecond
        wlog['__r_seconds'] = received_at.second
        wlog['__r_ms'] = received_at.microsecond
        wlog['date'] = date.replace(tzinfo=UTC, second=0, microsecond=0)
        received_at = received_at.replace(tzinfo=UTC, second=0, microsecond=0)
        wlog['received_at'] = self._utc_to_string(received_at)
    
    def _utc_to_string(self,data):
        """
        convert utc to string
        @param data: utc data
        @return: the string value of utc
        """
        try:
            value = solr.core.utc_to_string(data)
        except:
            pst = tz.gettz('Europe/Paris')
            value = value.replace(tzinfo=pst)
            value = solr.core.utc_to_string(data)
        return value

    def index(self,data):
        """
        index data in solr
        @param data: data to index into solr
        @return: true if indexing succeed 
        """
        self.update_wlog_for_solr(data) 
        for key, value in data.items():
            if isinstance(value,datetime):
                data[key] = self._utc_to_string(value)
          
        try:
            print "json:", data
            self.solr.update([data],commit=False)
            return True
        except Exception, e:
            log.msg("WARNING unable to index %s due to : %s" % (data,e))
            return False


# Yoinked from python docs
ZERO = timedelta(0)
class Utc(tzinfo):
    """UTC tzinfo instance
    """
    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO
UTC = Utc()


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
    lc = LogConsumer(znode_path, zc, zcrq, "resilience21", normalizer)
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
    
    zc = RetryClient(ZookeeperClient(zkAddr))
    d = zc.connect()
    d.addCallback(cb_connected, zc, normalizer)
    d.addErrback(log.msg)
    
    reactor.run()
    
if __name__ == "__main__":
    main()
