'''
Created on 1 juil. 2013

@author: lahoucine
'''
from zope.interface import Interface, implements
from pymongo import Connection
from  pymongo.errors import AutoReconnect
import gridfs
from resilience.zookeeper.configure.config import Config
import uuid
import time
from resilience.twisted.server.httpsServer import LogCollect
from twisted.python import log


class IFileStorage(Interface):
    '''
    Interface indicating that this object is a storage object.
    
    A storage object is used to store data in a defined file system.
    '''
    
    def initStorage(self):
        pass    
    
    def newFile(self, resource):
        pass
    
    def writeData(self):
        '''
        write data to the storage object
        '''
        pass
    
    def readData(self):
        '''
        Read data from the storage object
        '''
        pass
    
class IFileStorageZk(Interface):
    pass
    
class MongoGridFs(object):
    
    implements(IFileStorage)
    
    
    def __init__(self, mongoAdd, mongoPort, dbName):
        self.mongoAdd = mongoAdd
        self.mongoPort = mongoPort
        self.dbName = dbName
        self.db = None    
        self.mongofs = None
        
    def newFile(self, resource):
        file_name = 'log%s_%s.log' % (str(uuid.uuid4()), int(time.time()))    
        try:
            fileDescriptor = self.mongofs.new_file(filename = file_name, machine = resource)
            return (file_name, fileDescriptor, LogCollect.OK)
        except Exception,e :
            print "unvailable:",e
            return (None, None, LogCollect.UNAVAILABLE)
        
        
    def publishFile(self, fileDescriptor, filePath, linesAmount):
        fileId = fileDescriptor._id
        try:
            self.db.fs.files.update({'_id':fileId},{"$set":{"lines":linesAmount, "remLines":linesAmount, "position":0}})
        except:
            return (None, LogCollect.UNAVAILABLE)
        log.msg('Log chunk write on %s' % filePath)
        return (str(fileId), LogCollect.OK)
        
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
    
    
    
    def writeData(self):
        pass
    
    def readData(self):
        pass
    
    

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
    
     