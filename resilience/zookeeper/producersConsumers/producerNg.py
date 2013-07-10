#!/usr/bin/python
'''
Wallix

@author: Lahoucine BENLAHMR
@contact: lbenlahmr@wallix.com ben.lahoucine@gmail.com
'''
from OpenSSL import SSL
import sys
import os
import time
import uuid
from twisted.python import log
from twisted.internet import reactor, defer, ssl
from txzookeeper.client import ZookeeperClient
from txzookeeper.retry import RetryClient
from txzookeeper.queue import ReliableQueue
from twisted.internet import task
import thread
from resilience.twisted.server.httpsServer import initServerFactory
from resilience.twisted.server.httpsServer import LogCollectHandler as LogCollect
from pymongo import Connection
import gridfs
from  pymongo.errors import AutoReconnect
import argparse
from resilience.zookeeper.configure.config import Config
from resilience.zookeeper.DataStorage import MongoGridFs
log.startLogging(sys.stdout)


COUNT = 0
MAX_LINE = 500
MAX_WAIT = 15.0 # secondes
LINE_PROD = {}
FILE_PATH = {}
FILE_D = {}
TIMERS = {}

class LogProducer():
    """
      This class represent a Producer. Its role is to add tasks to the queue 
    for consuming. Each task contain a file id.
    The LogProducer use an HTTPS Server to Collect Logs lines and store them
    in a distributed file system (GridFS). 
    The LogProducer can process multiple source of collect using the above
    informations:
    
    -FILE_PATH: the current file name (or the file path) of the source "i" 
    -FILE_D: the current file descriptor of the source "i"
    -TIMERS: the source "i" descriptor life's time. After the end of
             this timer, the source "i" file's descriptor will be closed, And
             the id of this file will be added in the Queue
             
    -LINE_PROD: Used to count lines stored in a file
    -MAX_LINE: if LINEE_PROD of source "i" is equal to MAX_LINE, the file
                descriptor (FILE_D) of this source will be closed. And a task 
                will be added to the Queue
    """

    def __init__(self, znode_path, zk, zcrq, storage):
        """Initialization of the LogProducer
        @param znode_path:
        @param zk: Zookeeper client instance  
        @param zcqr: ReliableQueue instance 
        @param mongodb: MongoDB database name
        """ 
        #TODO: remove znode_path param
        self.znode_path = znode_path
        self.zcrq = zcrq
        self.zk = zk
        self.conf = Config(zk)
        self.storage = storage

   
         
       
    def _verify_delay(self,name):
        """ verify if the source "name" reached the timeOut and call commit
        @param name: name of the source collection
        """
        global LINE_PROD
        log.msg("Time Out")
        if not LINE_PROD[name] == 0:
            self.commit(name) 
            
    def _check_timer(self,name):
        """
        Initialize and manage timers 
        @param name: name of the source collection 
        """
        global TIMERS
        
        TIMERS[name] = TIMERS.get(name,None)
        if not TIMERS[name]:
            TIMERS[name] = task.LoopingCall(self._verify_delay, name)
            
        if not TIMERS[name].running:
            print("Timer %s is not running" % name)
            TIMERS[name].start(MAX_WAIT,False)
        else:
            TIMERS[name].reset() 
                              
    
    def produce(self, logLine, name):
        """
        initialize all fields (LINE_PROD, FILE_PATH, FILE_D) and call
        different function to process log lines
        @param logLine: logLine to proceed 
        @param name: name of the source collection 
        @return: LogCollect.OK if producing succeeded 
        """
        global LINE_PROD
        global FILE_PATH
        global FILE_D
        global MAX_LINE      
        #TODO: verify if zookeeper is up
        self._check_timer(name)
        LINE_PROD[name] = LINE_PROD.get(name,0)
        status = LogCollect.OK
        if LINE_PROD[name] == 0:
            try:
                FILE_PATH[name], FILE_D[name], status = self.storage.newFile(name)
                if not status == LogCollect.OK:
                    return status
            except:
                return LogCollect.UNAVAILABLE
            
        logLine = logLine.rstrip("\n") # to avoid blank lines on created file
        try:
            FILE_D[name].write('%s\n' % logLine)
        except:
            return LogCollect.UNAVAILABLE
        LINE_PROD[name] += 1
        if LINE_PROD[name] == MAX_LINE:
            self.commit(name)
        return LogCollect.OK
        
    def commit(self,name):
        """Close file descriptor, stop timer of the source "name" and  call
        _gridfs_publish 
        @param  name: name of the source collection
        @return: LogCollect.OK if operation succeed
        """
        global FILE_D
        global FILE_PATH
        global TIMERS
        
        TIMERS[name].stop()
        fileDescriptor = None
        linesAmount = None
        filePath = None
        
        try:        
            FILE_D[name].close()
            fileDescriptor = FILE_D[name]
            FILE_D[name] = None
            linesAmount = LINE_PROD[name]
            LINE_PROD[name] = 0
            filePath = FILE_PATH[name]            
        except:
            log.msg('Can not close file: %s' % FILE_PATH[name]) 
        fileid, status = self.storage.finalizeFile(fileDescriptor, filePath, linesAmount)
        if status == LogCollect.OK:
            self.publish(fileid)
        return status 
        
    def publish(self, filepath):
        """ Publish the collected file in the Queue
        @param filepath: file Id 
        """
        d = self.zcrq.put(filepath)
        d.addCallback(lambda x: log.msg('Log chunk published on queue : %s' % x))
        d.addErrback(lambda x: log.msg('Unable to publish chunk on queue : %s' % x))


def initQueue(zc, storage):
    def _err(error):
        log.msg('Queue znode seems to already exists : %s' % error)
    znode_path = '/log_chunk_produced42'
    d = zc.create(znode_path)
    d.addCallback(lambda x: log.msg('Queue znode created at %s' % znode_path))
    d.addErrback(_err)
    zcrq = ReliableQueue(znode_path, zc, persistent = True)
    lp = LogProducer(znode_path,zc, zcrq, storage)
    return lp

def cb_connected(useless, zc, host, port, certDir):
    """
    Create a reliable Queue and an HTTPS server
    and Start an instance of producer
    @param zc:
    @param mongodb: MongoDB dataBase name 
    @param host: address of embedded HTTPS server to create 
    @param port: port of embedded HTTPS server to create 
    """ 
    log.msg("Connected To Zookeeper")
    mongodb = 'resilience21'
    storage = MongoGridFs(mongodb, Config(zc), reactor)
    lp = initQueue(zc, storage)
    #factory = initServerFactory(lp, zc, reactor, ".", host, port)
    #config_ssl(zc, factory, host, port)
    initServerFactory(lp, zc, reactor, certDir, host, port )
           
def main():
    """
    bootstrap function
    """
    params = sys.argv[1:]
    parser = argparse.ArgumentParser(description='Log producer with embedded https server ')
    parser.add_argument('-z','--zkServer',help='address of the zookeeper server', 
                                          default="localhost:2181", required=True)
    parser.add_argument('-a','--host',help='the hostname to bind to, defaults to localhost', 
                                          default="localhost", required = False)
    parser.add_argument('-p','--port',help='the port to listen in', type=int, 
                                          default="8991", required=False)
    parser.add_argument('-c','--certDir',help='the  certificat and key diretory', 
                                          default="/tmp/", required=False)
    
    args = parser.parse_args(params)
    zkAddr = args.zkServer
    host = args.host
    port = args.port
    certDir = args.certDir
    
    zc = RetryClient(ZookeeperClient(zkAddr))
    d = zc.connect()
    d.addCallback(cb_connected, zc, host, port, certDir)
    d.addErrback(log.msg)
    reactor.run()
    
   
if __name__ == "__main__":
    main()
