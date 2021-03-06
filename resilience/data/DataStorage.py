from zope.interface import Interface, implements
from pymongo import Connection
from  pymongo.errors import AutoReconnect, OperationFailure
import gridfs
from resilience.zookeeper.configure.config import Config
import uuid
import time
from resilience.twisted.server.status import Status
from twisted.python import log
import bson
import sys

class IFileStorage(Interface):
    '''
    Interface indicating that this object is a storage object.
    
    A storage object is used to store data in a defined file system.
    '''   
    def newFile(self, resource):
        """
        this method create a new file in the storage for a giving resource name.
        it must return a file descriptor witch must expose the following methods:
        - write(data) where data is a string
        - close()
        """
        
    
    def finalizeFile(self, fileDescriptor, filePath, linesAmount):
        pass
    
    def getFile(self, item):
        """
        this methode returns a file descriptor for givin item.
        depending in the storage the item can be anything 
        for example a filePath or the file id ...
        the file descriptor return must expose a readline methode
        to read a file ligne by ligne
        """
    
    def saveCurrentPosition(self, fileItem):
        """
        the aim of this method is to save the current position of a giving
        fileItem represented by a file descriptor
        """

class MongoGridFs(object):
    
    implements(IFileStorage)
    
    
    def __init__(self, dbName, conf, reactor, user=None, password=None):
        self.conf = conf
        self.db = None    
        self.mongofs = None
        self._init_mongo(dbName)
        self.reactor = reactor
        self.user = user
        self.password = password
        
    def newFile(self, resource):
        file_name = 'log%s_%s.log' % (str(uuid.uuid4()), int(time.time()))    
        try:
            fileDescriptor = self.mongofs.new_file(filename = file_name, machine = resource)
            return (file_name, fileDescriptor, Status.OK)
        except Exception,e :
            print "unvailable:",e
            return (None, None, Status.UNAVAILABLE)
        
        
    def finalizeFile(self, fileDescriptor, filePath, linesAmount):
        fileId = None
        #import pdb; pdb.set_trace()
        try:
            fileId = fileDescriptor._id
            self.db.fs.files.update({'_id':fileId},{"$set":{"lines":linesAmount, "remLines":linesAmount, "position":0}})
        except:
            return (None, Status.UNAVAILABLE)
        log.msg('Log chunk write on %s' % filePath)
        return (str(fileId), Status.OK)
    
    def getFile(self, item, zkitem = True, seek=True):
        try:
            if zkitem:
                fileId = bson.ObjectId(item.data)
            else:
                fileId = bson.ObjectId(item)
                
            fileItem = self.mongofs.get(fileId)
            result = self.db.fs.files.find_one({'_id':fileId}, {'position':1, '_id':0})
            position = result['position']
            print "POSITION:", position
            if seek:
                fileItem.seek(position)
            return fileItem
        except Exception, e:
            print e
            return None
                
    def saveCurrentPosition(self, fileItem):
        self.db.fs.files.update({'_id': fileItem._id},{"$set":{"position":fileItem.tell()}})
    
    def updateRemLines(self, fileId):
        if type(fileId) != bson.objectid.ObjectId:
            fileId = bson.ObjectId(fileId)
        self.db.fs.files.update({'_id':bson.ObjectId(fileId)},{"$inc":{"remLines":-1}})
        
    def getRemLines(self, fileId):
        if type(fileId) != bson.objectid.ObjectId:
            fileId = bson.ObjectId(fileId)
        res = self.db.fs.files.find_one({'_id':fileId})
        return res['remLines']
        
    
    def delete(self, fileId):
        self.mongofs.delete(bson.ObjectId(fileId))
    
    def purge(self):
        res = self.db.fs.files.find({"remLines": 0})
        for r in res:
            self.mongofs.delete(r["_id"])
    
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
                if self.user and self.password:
                    try:
                        adminDB = connection['admin']
                        adminDB.add_user(self.user, self.password)
                    except:
                        pass
                    try:
                        self.db.authenticate(self.user, self.password)
                    except:
                        print "authentification to mognodb failed"
                try:
                    self.mongofs = gridfs.GridFS(self.db)
                except OperationFailure, e:
                    print "mongodb:", e
                    print "Please retry later!"
                    if self.reactor.running:
                         self.reactor.stop()
                    
                        
                print "connected to mongodb: %s:%s" %  (mongoAdd, mongoPort)
            except AutoReconnect, e:
                print "mongodb:", e
                self.reactor.callLater(2, _connect, mongoAdd, mongoPort, limit)#TODO: test callLater
     
                
        def _call(m):
            """Retrieve MonogDB configuration from Zookeeper
            and call local function _connect              
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
                self.mongofs = None
                self.bd = None
        self.conf.get_mongod_all(_call)
    
     
