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
from resilience.twisted.server.httpsServer import LogCollect
from pymongo import Connection
import gridfs
from  pymongo.errors import AutoReconnect
import argparse

from resilience.zookeeper.configure.config import Config
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

    def __init__(self, znode_path, zk, zcrq, mongodb):
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
        self.db = None    
        self.mongofs = None
        self._init_mongo(mongodb)

        
               
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
                time.sleep(2)
                #to not reach python's recursion limit - 100
                if limit < (sys.getrecursionlimit() - 100):
                    _connect(mongoAdd, mongoPort, limit+1)
                
        def _call(m):
            """Retrieve MonogDB configuration from Zookeeper
            and call _connect local function             
            """
            if m:
                mg = m[0]
                #TODO:  catch exception, (need more than one value)
                #mongoAdd, mongoPort = mg.split(":")
                mongoAdd, sep, mongoPort = mg.rpartition(":")
                if mongoAdd == '':
                    log.msg('mongo: %s is not a correct address', mg)
                    return 
                mongoAdd = '[%s]' % mongoAdd
                print "conf mongo", mongoAdd, mongoPort
                _connect(mongoAdd, mongoPort,0)
            else:
                print "0 Mongo server founded"
                self.mongofs = None
                self.bd = None
                    
        self.conf.get_mongod_all(_call)
         
       
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
                         
    
    def _gridfs_write(self,name):
        """
        Create a new file into GridFS for the source collection "name"
        @param name: name of the source collection
        @return: LogCollect.Ok if operation succeed, LogColelct.UNAVAILABLE 
                 otherwise
        """
        global LINE_PROD
        global FILE_PATH
        global FILE_D
      
        file_name = 'log%s_%s.log' % (str(uuid.uuid4()), int(time.time()))    
        FILE_PATH[name] = file_name
        try:
            FILE_D[name] = self.mongofs.new_file(filename = file_name, machine = name)
            return LogCollect.OK
        except Exception,e :
            print "unvailable:",e
            return LogCollect.UNAVAILABLE
       
    
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
                status = self._gridfs_write(name)
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
        try:        
            FILE_D[name].close()
        except:
            log.msg('Can not close file: %s' % FILE_PATH[name]) 
        return self._gridfs_publish(name)
            
    def _gridfs_publish(self,name):
        """Update the file's meta-data of the source "name".
        And call publish
        @param name: source collection name
        @return: LogCollect.OK if operation succeed, LogCollect.UNAVAILABLE 
                otherwise.
        """
        global FILE_D
        global FILE_PATH      
        global LINE_PROD

        id =  FILE_D[name]._id
        FILE_D[name] = None
        count = LINE_PROD[name]
        try:
            self.db.fs.files.update({'_id':id},{"$set":{"lines":count,"remLines":count, "position":0}})
        except:
            return LogCollect.UNAVAILABLE
                  
        log.msg('Log chunk write on %s' % FILE_PATH[name])
        LINE_PROD[name] = 0
        #TODO: verify publishing
        self.publish(str(id))
        return LogCollect.OK
        
    def publish(self, filepath):
        """ Publish the collected file in the Queue
        @param filepath: file Id 
        """
        d = self.zcrq.put(filepath)
        d.addCallback(lambda x: log.msg('Log chunk published on queue : %s' % x))
        d.addErrback(lambda x: log.msg('Unable to publish chunk on queue : %s' % x))


global count 
count = 0
def config_ssl(zc, factory, host, port):
    global count
    cfg = Config(zc)
    path = "."
    cfg.get_key_ca(path)
    cfg.get_certificat_ca(path)
    keyfile = os.path.join(path,"ca.key")
    certfile = os.path.join(path, "ca.cert")
    #verfy if ca files was retrieved from zookeeper                                                               
    if not os.path.exists(keyfile) or not os.path.exists(certfile):
        if count > 10:
            print "producer will be stoped..."
            reactor.callFromThread(reactor.stop)
            return 
        count += 1
        reactor.callLater(2, config_ssl, zc, factory, host, port)
        return 
    

    try:
        sslContext = ssl.DefaultOpenSSLContextFactory(keyfile,
                                                  certfile,
                                                 )
        log.msg("SSL certificat configured")
        ctx = sslContext.getContext()
        reactor.listenSSL(port, # integer port                                                                                         
                          factory, # our site object                                                                                   
                          contextFactory = sslContext,
                          interface = host
                          )
    except Exception, e:
        log.msg(e)
        reactor.stop()
        return


def initQueue(zc, mongodb):
    def _err(error):
        log.msg('Queue znode seems to already exists : %s' % error)
    znode_path = '/log_chunk_produced42'
    d = zc.create(znode_path)
    d.addCallback(lambda x: log.msg('Queue znode created at %s' % znode_path))
    d.addErrback(_err)
    zcrq = ReliableQueue(znode_path, zc, persistent = True)
    lp = LogProducer(znode_path,zc, zcrq, mongodb)
    return lp

def cb_connected(useless, zc , mongodb, host, port):
    """
    Create a reliable Queue and an HTTPS server
    and Start an instance of producer
    @param zc:
    @param mongodb: MongoDB dataBase name 
    @param host: address of embedded HTTPS server to create 
    @param port: port of embedded HTTPS server to create 
    """ 
    log.msg("Connected To Zookeeper")
    lp = initQueue(zc, mongodb)
    factory = initServerFactory(lp, zc)
    config_ssl(zc, factory, host, port)

           
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
    
    args = parser.parse_args(params)
    mongodb = 'resilience21'
    zkAddr = args.zkServer
    host = args.host
    port = args.port
    
    zc = RetryClient(ZookeeperClient(zkAddr))
    d = zc.connect()
    d.addCallback(cb_connected, zc, mongodb, host, port)
    d.addErrback(log.msg)
    reactor.run()
    
   
if __name__ == "__main__":
    main()
