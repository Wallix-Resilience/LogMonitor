#!/usr/bin/python
from OpenSSL import SSL
import sys
import os
import random
import time
import uuid
from twisted.python import log
from twisted.internet import reactor, defer, ssl
from txzookeeper.client import ZookeeperClient
from txzookeeper.retry import RetryClient
from txzookeeper.queue import ReliableQueue
#
from twisted.internet import task
import thread
#
from resilience.twisted.server.httpsServer import initServerFactory
import time
#
from pymongo import Connection
import gridfs

log.startLogging(sys.stdout)

COUNT = 0
MAX_LINE = 500
MAX_WAIT = 15.0 # secondes
LINE_PROD = {}
FILE_PATH = {}
FILE_D = {}
TIMERS = {}

class LogProducer():

    def __init__(self, datadir, znode_path, zcrq):
        log.msg("log producer init")
        self.datadir = datadir
        self.znode_path = znode_path
        self.zcrq = zcrq
        self._init_mongo("resilience6")
    
    def _init_mongo(self,dbName = "resilience"):
        connection = Connection()
        self.db = connection[dbName]
        self.mongofs = gridfs.GridFS(self.db)      
       
        
    def _verify_delay(self,name):
        global LINE_PROD
        log.msg("Time Out")
        if not LINE_PROD[name] == 0:
            self.commit(name) 
            
    def _check_timer(self,name):
        global TIMERS
        
        TIMERS[name] = TIMERS.get(name,None)
        if not TIMERS[name]:
            print "timers null"
            TIMERS[name] = task.LoopingCall(self._verify_delay, name)
            
        if not TIMERS[name].running:
            print("Timer %s is not running" % name)
            TIMERS[name].start(MAX_WAIT,False)
        else:
            TIMERS[name].reset() 
            
    def _local_write(self,name):
        global LINE_PROD
        global FILE_PATH
        global FILE_D
        global MAX_LINE  
      
        file_name = 'log%s_%s.log' % (str(uuid.uuid4()), int(time.time()))
        nodeDir = os.path.join(self.datadir,name)
        if not os.path.isdir(nodeDir):
            os.mkdir(nodeDir)
            
        date = time.localtime(time.time())
        year = date[0]
        month = date[1]
        day = date[2]
        subDirName = "%s-%s-%s" % (day, month, year)
        subDirDay =   os.path.join(self.datadir,name,subDirName)
        if not os.path.isdir(subDirDay):
            os.mkdir(subDirDay)
                
        filepath = os.path.join(self.datadir, name, subDirDay, file_name)
        FILE_PATH[name] = filepath
        FILE_D[name] = file(filepath, 'w',0)
        print "keyss: ",  FILE_D.keys()
            

      
    
    def _gridfs_write(self,name):
        global LINE_PROD
        global FILE_PATH
        global FILE_D
      
        file_name = 'log%s_%s.log' % (str(uuid.uuid4()), int(time.time()))    
        FILE_PATH[name] = file_name
        FILE_D[name] = self.mongofs.new_file(filename = file_name, machine = name)    
        print "keys:", FILE_D.keys()

    
    def produce(self, logLine, name):
        global LINE_PROD
        global FILE_PATH
        global FILE_D
        global MAX_LINE      

        self._check_timer(name)
        
        LINE_PROD[name] = LINE_PROD.get(name,0)
        print "LINEEEE PRODDD ", LINE_PROD[name]
        if LINE_PROD[name] == 0:
            self._gridfs_write(name)
            
        print "keys:", FILE_D.keys()
        logLine = logLine.rstrip("\n") # to avoid blank lines on created file
        FILE_D[name].write('%s\n' % logLine)
        #log.msg("log linee %s" % logLine)
        LINE_PROD[name] += 1
        print "LINE PRODD Plusssss ", LINE_PROD[name]
        if LINE_PROD[name] == MAX_LINE:
           print "committttttt"
           self.commit(name)
        
    def commit(self,name):
        global FILE_D
        global FILE_PATH
        global TIMERS
        
        TIMERS[name].stop()        
        FILE_D[name].close() 
        self._gridfs_publish(name)
         
    def publish(self, filepath):
        d = self.zcrq.put(filepath)
        d.addCallback(lambda x: log.msg('Log chunk published on queue : %s' % x))
        d.addErrback(lambda x: log.msg('Unable to publish chunk on queue : %s' % x))
        
    def _local_publish(self,name):
        global FILE_D
        global FILE_PATH
        global LINE_PROD

        FILE_D[name] = None
        log.msg('Log chunk write on %s' % FILE_PATH[name])
        LINE_PROD[name] = 0
        self.publish(str(FILE_PATH[name]))
    
    def _gridfs_publish(self,name):
        global FILE_D
        global FILE_PATH      
        global LINE_PROD

        id =  FILE_D[name]._id
        FILE_D[name] = None
        count = LINE_PROD[name]
        self.db.fs.files.update({'_id':id},{"$set":{"lines":count,"remLines":count}})          
        log.msg('Log chunk write on %s' % FILE_PATH[name])
        LINE_PROD[name] = 0
        self.publish(str(id))
        
    



def cb_connected(useless, zc, datadir):
    def _err(error):
        log.msg('Queue znode seems to already exists : %s' % error)
    znode_path = '/log_chunk_produced26'
    d = zc.create(znode_path)
    d.addCallback(lambda x: log.msg('Queue znode created at %s' % znode_path))
    d.addErrback(_err)
    zcrq = ReliableQueue(znode_path, zc, persistent = True)
    ############   
    lp = LogProducer(datadir, znode_path, zcrq)
    factory = initServerFactory(lp)
    privKey = os.path.abspath('../../../ssl/ca/privkey.pem')
    caCert = os.path.abspath('../../../ssl/ca/cacert.pem')
    sslContext = ssl.DefaultOpenSSLContextFactory(privKey, 
                                                  caCert,
                                                 )
    #
    def _verifyCallback(connection, x509, errnum, errdepth, ok):
        print "verify digest: ", x509.digest("md5")
        if not ok:
            log.msg('invalid cert from subject:%s' % x509.get_subject())
            return True
        else:
            log.msg("Certs are fine: %s " % x509.get_subject())
        return True
    
    ctx = sslContext.getContext()

    ctx.set_verify(
        SSL.VERIFY_PEER | SSL.VERIFY_FAIL_IF_NO_PEER_CERT,
        _verifyCallback
        )
 
    certVerif = os.path.abspath('../../../ssl/certs/ss_cert_c.pem')
    ctx.load_verify_locations(certVerif)
    
    reactor.listenSSL(8990, # integer port 
                      factory, # our site object
                      contextFactory = sslContext,
                      )
    #reactor.listenTCP(8880,factory)
    ############
    
if __name__ == "__main__":
    #sys.path.append("/home/lahoucine/workspace/resilience/resilience/twisted/server") 
    datadir = '/tmp/rawdata'
    if not os.path.isdir(datadir):
        os.mkdir(datadir)
    zc = RetryClient(ZookeeperClient("127.0.0.1:2181"))
    d = zc.connect()
    d.addCallback(cb_connected, zc, datadir)
    d.addErrback(log.msg)
    reactor.run()
