#!/usr/bin/python

import sys
from twisted.web import http
from twisted.web.server import Site
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.internet import reactor
from twisted.python import log
from time import *
from resilience.zookeeper.configure.config import Config
from pymongo import Connection
from  pymongo.errors import AutoReconnect
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto import Random
from resilience.data.DataIndexer import SolrIndexer
from OpenSSL import crypto, SSL
from socket import gethostname
from os.path import exists, join
from twisted.internet import ssl
import simplejson as json
from resilience.data.DataStorage import MongoGridFs
from resilience.twisted.server.status import Status
from netaddr import IPAddress
log.startLogging(sys.stdout)

class RootResource(Resource):
    def __init__(self, credentialStore, producer, indexer, storage):
        Resource.__init__(self)
        self.putChild('register', RegisterHandler(credentialStore))
        self.putChild('change', ChangeHandler(credentialStore))
        self.putChild('send', LogCollectHandler(producer, credentialStore))
        self.putChild('search', searchHandler(credentialStore, indexer))
        self.putChild('remove', RemoveHandler(credentialStore))
        self.putChild('getFile', GetFileHandler(credentialStore, storage))
        self.putChild('getSources', ListSourceHandler(credentialStore))
        self.putChild('deleteLogs', DeleteLogsHandler(credentialStore, storage, indexer))
        self.putChild('deleteFile', DeleteFileHandler(credentialStore, storage, indexer))        
        self.putChild('purge', PurgeStorageHandler(storage))
        
    def getChild(self, name, request):
        if name == '':
            return self
        return Resource.getChild(self, name, request)
    
    def render_GET(self, request):
        names = self.listNames()
        body = """<html><body>"""
        for n in names:
            body+= "<a href='%s'>%s</a></br>" % (n, n)
        body+="""</body></html> """
        return body
    
class RegisterHandler(Resource):
	
    def __init__(self, credentialStore):
        self.cred = credentialStore
		
    def render_POST(self, request):
        #tester si user et pass sont fournis ...
        user = request.args['user'][0]
        password = request.args['password'][0]
        sourceName = request.args['source'][0]
        ip = request.args['ip'][0]
        print self.cred.checkUser(user, password)
        if self.cred.checkUser(user, password):
            rep =  self.cred.addSource(sourceName, ip)
            if not rep:
                request.setResponseCode(http.CONFLICT)
                return ""
            return rep
        request.setResponseCode(http.UNAUTHORIZED)
        return ""
        
    def render_GET(self, request):
        request.setResponseCode(http.NOT_FOUND)
        return """
            <html><body>use post method for direct registration or form below:<br><br>
            <form action='/register' method=POST>
            Admin    : <input type='text' name='user'><br>
            Password : <input type='password' name='password'><br>
            Source name to add : <input type='text' name='source'><br>
            Source IP : <input type='text' name='ip'><br>
            <input type='submit'>
            </body></html>      
        """

class RemoveHandler(Resource):
    
    def __init__(self, credentialStore):
        self.cred = credentialStore
        
    def render_POST(self, request):
        user = request.args['user'][0]
        password = request.args['password'][0]
        sourceName = request.args['source'][0]
        print self.cred.checkUser(user, password)
        if self.cred.checkUser(user, password):
            if not self.cred.removeSource(sourceName):
                request.setResponseCode(http.BAD_REQUEST)
            else:
                return "Deleted with success"
    
        request.setResponseCode(http.UNAUTHORIZED)
        return ""
    
        
    def render_GET(self, request):
        request.setResponseCode(http.NOT_FOUND)
        return """
            <html><body>use post method for direct remove or form below:<br><br>
            <form action='/remove' method=POST>
            Admin    : <input type='text' name='user'><br>
            Password : <input type='password' name='password'><br>
            Source name to remove : <input type='text' name='source'><br>
            <input type='submit'>
            </body></html>      
        """
class ChangeHandler(Resource):

    def __init__(self, credentialStore):
        Resource.__init__(self)
        self.cred = credentialStore

    
    def render_POST(self, request):
        oldUserName = request.args["oUser"][0]
       	oldPassword = request.args["oPass"][0]
	newUserName = request.args["nUser"][0]
	newPassword = request.args["nPass"][0]
        if self.cred.checkUser(oldUserName, oldPassword) :
            if self.cred.changeUserAndPassword(oldUserName, oldPassword, newUserName, newPassword):
                return "Change Succed"
            else:
                return  "Error"
        request.setResponseCode(http.UNAUTHORIZED)    
        return "Error: UserName or/and password invalid !?"

    def render_GET(self, request):
	return """
	<html><body>use form below to change default admin username and password:<br><br>
	<form action='/change' method=POST>
	Admin user Name : <input type="text" name="oUser"><br>
	Password : <input type="password" name="oPass"><br>
	New admin user name : <input type="text" name="nUser"><br>
	New password : <input type="password" name="nPass"><br>
	<input type="submit">
	</body></html>
	"""
    
    
class searchHandler(Resource):
    
    def __init__(self, credential, indexer):
        self.indexer = indexer
        self.cred = credential
    
    def render_POST(self, request):
        user = request.args['user'][0]
        password = request.args['password'][0]
        query = request.args['query'][0]
        rows = 500
        if "rows" in request.args.keys():
            rows = request.args['rows'][0] 
        if self.cred.checkUser(user, password):
            request.setHeader('Content-Type', 'Content-Type: text/plain')
            request.setHeader('charset', 'US-ASCII')
            #request.setHeader('Transfer-Encoding', 'chunked')
            rows = int(rows)
            return json.dumps(self.indexer.search(query, rows), sort_keys = False, indent = 4)
            
    def render_GET(self, request):
        request.setResponseCode(http.NOT_FOUND)
        return """
            <html><body>use post method for direct search or form below:<br><br>
            To make a search query you have to use the lucen query language: http://lucene.apache.org/core/2_9_4/queryparsersyntax.html<br>
the seach engine is tag based. All indexed logs contains the following tags:<br><br>                                                
date: issue date of the log, expressed in the UTC time<br>
received_at: date the log was received by the WAB Report Manager, expressed in UTC time<br>
source: network address or host name of the machine that issued the log<br>
raw: the complete, raw log line<br>
body: the message describing the event for which notification was given<br>
program: may appear if a piece of information regarding the program issuing the log was detected<br>
uuid: unique identifier associated with the log line<br><br>

for more possible tags please refer to normalizers descriptions https://github.com/wallix/pylogsparser/tree/master/normalizers.<br>
to make a full text search use the tag body. example:<br> body:linux<br><br>
            <form action='/search' method=POST>
            Admin    : <input type='text' name='user'><br>
            Password : <input type='password' name='password'><br>
            query : <input type='text' name='query'><br>
            rows : <input type='text' name='rows' value=500><br>
            <input type='submit'>
            </body></html>      
        """
        
class GetFileHandler(Resource):
    
    def __init__(self, credential, storage):
        self.cred = credential
        self.storage = storage
    
    def render_POST(self, request):
        user = request.args['user'][0]
        password = request.args['password'][0]
        file_id = request.args['fileID'][0]
        if self.cred.checkUser(user, password):
            file_item = self.storage.getFile(file_id,zkitem=False, seek=False)
            
            if file_item:
                file_data = file_item.read()
                request.setHeader('Content-Type', 'Content-Type: text/plain')
                request.responseHeaders.setRawHeaders('Content-Disposition', ['attachment; filename="%s"' % 
                                                                                file_item.name])
                return file_data
            request.setResponseCode(http.NOT_FOUND)
            return "File not found"
        request.setResponseCode(http.UNAUTHORIZED)    
        return "Error: UserName or/and password invalid !?"
    
    
    def render_GET(self, request):
        return """
            <html><body>use post method to get a file directly or use the form below:<br><br>
            <form action='/getFile' method=POST>
            Admin    : <input type='text' name='user'><br>
            Password : <input type='password' name='password'><br>
            FileID : <input type='text' name='fileID'><br>
            <input type='submit'>
            </body></html>      
        """
    
class ListSourceHandler(Resource):
    
    def __init__(self, credential):
        self.cred = credential
        
    def render_POST(self, request):
        user = request.args['user'][0]
        password = request.args['password'][0]
        if self.cred.checkUser(user, password):
            items = self.cred.listSource()
            request.setHeader('Content-Type', 'Content-Type: text/plain')
            request.setHeader('charset', 'US-ASCII')
            if items:
                return  json.dumps(items, sort_keys = False, indent = 4)
            else:
                return '[]'
        request.setResponseCode(http.UNAUTHORIZED)    
        return "Error: UserName or/and password invalid !?"  
    
    def render_GET(self, request):
        return """
            <html><body>use post method to get the liste of declared sources or use the form below:<br><br>
            <form action='/getSources' method=POST>
            Admin    : <input type='text' name='user'><br>
            Password : <input type='password' name='password'><br>
            <input type='submit'>
            </body></html>      
        """
   
   
class DeleteLogsHandler(Resource):
    
    def __init__(self, credential, storage, indexer):
        self.cred = credential
        self.storage = storage
        self.indexer = indexer
        
    def render_POST(self, request):
        user = request.args['user'][0]
        password = request.args['password'][0]
        query = request.args['query']       
        if self.cred.checkUser(user, password):
            propagate = False
            if 'propagate' in request.args.keys():
                propagate = True
            print propagate
            self.deleteLogs(query, propagate)
            return 'ok'
        
        request.setResponseCode(http.UNAUTHORIZED)    
        return "Error: UserName or/and password invalid !?"
    
    def render_GET(self, request):
        return """
            <html><body>use post method to delete logs directly or use the form below:<br><br>
            <form action='/deleteLogs' method=POST>
            Admin    : <input type='text' name='user'><br>
            Password : <input type='password' name='password'><br>
            query : <input type='text' name='query'><br>
            Propagate to Data storage<INPUT type="checkbox" name="propagate" value="True">
            <input type='submit'>
            </body></html>"""
     
    def deleteLogs(self, query, propagate = False):
        docs = self.indexer.search(query)
        for doc in docs:
            print doc
            fileid = doc['fileid']
            print "fileID", fileid
            uuid = "uuid:%s" % doc['uuid']
            self.indexer.delete_by_query(uuid)
            self.storage.updateRemLines(fileid)
            remLines = self.storage.getRemLines(fileid)
            if propagate:
                print "popagate: ", remLines
                if remLines == 0:
                    print "delete"
                    self.storage.delete(fileid)
                    
                    
class DeleteFileHandler(Resource):
    
    def __init__(self, credential, storage, indexer):
        self.cred = credential
        self.storage = storage
        self.indexer = indexer
        
    def render_POST(self, request):
        user = request.args['user'][0]
        password = request.args['password'][0]
        fileid = request.args['fileid'][0]     
        if self.cred.checkUser(user, password):
            propagate = False
            if 'propagate' in request.args.keys():
                propagate = True
            print propagate
            self.deleteFile(fileid, propagate)
            return 'ok'
        
        request.setResponseCode(http.UNAUTHORIZED)    
        return "Error: UserName or/and password invalid !?"
    
    def render_GET(self, request):
        return """
            <html><body>use post method to delete a file directly or use the form below:<br><br>
            <form action='/deleteLogs' method=POST>
            Admin    : <input type='text' name='user'><br>
            Password : <input type='password' name='password'><br>
            fileID : <input type='text' name='fileid'><br>
            Propagate to Data storage<INPUT type="checkbox" name="propagate" value="True">
            <input type='submit'>
            </body></html>"""
     
    def deleteFile(self, fileid, propagate = False):
        self.storage.delete(fileid)
        if propagate:
                query = "fileid:%s" % fileid
                self.indexer.delete_by_query(query)
                                       
                    
class PurgeStorageHandler(Resource):
    
    def __init__(self, storage):
        self.storage = storage
    
    def render_POST(self,request):
        user = request.args['user'][0]
        password = request.args['password'][0]
        if self.cred.checkUser(user, password):
            self.storage.purge()
    
    def render_GET(self, request):
        self.storage.purge()
    
        
class LogCollectHandler(Resource):
    
    OK = 200
    NO = 500
    UNAVAILABLE = 503
    
    isLeaf = True
    def __init__(self, producer, credentialStore):
        self.prod = producer   
        self.cred = credentialStore
     
    def render_POST(self, request):
        """
        process the HTTP POST requests an send a response code to the client:
        200 -> ok
        500 -> no
        503 -> Service Unavailable
        """
         
        postpath =  request.postpath # postpath contain the key of the client
        print postpath
        print request.path
        if len(postpath)!= 1 or postpath[0] == '': # if no giving key (postpath)
            request.setResponseCode(Status.NO) # send code response NO
            return ""
        key = postpath[0]
        ip = request.getClientIP()
        if not ip:
            ip = request.getClient()
        print "ip",ip
        log.msg( "session: %s"% request.getSession().uid)
        #startTime = time()
        sourceName = self.cred.checkSource(key, ip)
        if sourceName:
            code = logProcess(request, self.prod, sourceName)
            if code:
                request.setResponseCode(int(code))
            else:
                request.setResponseCode(Status.UNAVAILABLE)                
            try:	
                request.finish()
            except:
		pass
	else:
            request.setResponseCode(Status.NO)
            try:
                request.finish()
            except:
                pass   
        return NOT_DONE_YET  # we have to wait the response code 
        
       

def logProcess(request, producer, name):
    """
    process received log from HTTP POST request, using an instance of producer
    @param request: received request to process by an instance of producer
    @param producer: an instance of producer which will process received data
    @param name: the name of the collection source 
     
    @return: LogCollect.Ok if the process succeeded, 
    LogCollect.NO or LogCollect.UNAVAILABLE if not.
    """
    line = request.content.getvalue()
    if  line.strip():
        print "line: ",line
        code =  producer.produce(line, name)
        print code
        return code
    else:
        print "empty"
        return Status.OK
       

class CredentialStore():
	
    def __init__(self, conf):
       	self.store = None
       	self.users =  None
	self.sources = None
	self.conf = conf
        self._init_mongo()
	
			
		
    def changeUserAndPassword(self, oldUsername, oldPassword, username, password):
        try:
            shaNewPassword = SHA256.new(password)
            newPassword =  shaNewPassword.hexdigest()
            shaOldPassword = SHA256.new(oldPassword)
            oldPassword = shaOldPassword.hexdigest()
            self.users.update({'user': oldUsername, 'password': oldPassword}, {'user': username, 'password': newPassword})
            return True
        except:
            return False

    def addSource(self, name, ip):
        key = self._key_generator()
        exists = None
        try:
            exists = self.sources.find_one({"name": name}, {"name": 1})
        except Exception, e:
            print "Exception: ", e
            print "Try later!"
        if not exists :
            try :
                source = { "name" : name, "ip": ip, "key": key }
                self.sources.insert(source)
                return key
            except:
                return None				
        return None
    
    def removeSource(self, name):
        try:
            source = {"name" : name}
            self.sources.remove(source)
            return True
        except:
            return False
        
    def listSource(self):
        try:
            sources = list(self.sources.find())
            for item in sources:
                del item['_id']
            return sources
        except:
            return None
            
		
    def checkSource(self, key, ip):
        source = None
        try:
            source = self.sources.find_one({"key": key, "ip": ip})
            print "check source",ip ,"with key ", key
        except Exception, e:
            print "Exception: ", e
        if source:
            print source
            ipStored = IPAddress( source['ip'] )
            ipClient = IPAddress( ip )  
            if ipClient and ipStored == ipClient:
                return source["name"]
        return None
	
    def checkUser(self, user, password):
        sha = SHA256.new(password)
        exists = None
        try:
            exists = self.users.find_one({"user": user, "password": sha.hexdigest()})
        except Exception, e:
            print "Exception: ", e
        return exists is not None
	
    def _key_generator(self):
        """
        generate a key, by hashing an RSA key randomly generated.
        the hashing method used is sha2
        """
        random_generator = Random.new().read
        key = RSA.generate(1024, random_generator)
        exportedKey = key.exportKey()
        sha = SHA256.new()
        sha.update(exportedKey)
        hashk = sha.hexdigest()
        return hashk
            
            
            
    def _init_mongo(self):
        """Initialization of a MongoDB instance 
        """
        def _connect(mongoAdd, mongoPort, limit):
            """Connect producer to MongoDB and create a GridFs instance
            @param mongoAdd: MongoDB address 
            @param mongoPort: MongoDB port
            @param limit: initialized to zero for recursive call
            """
            try:
                mongo = Connection(mongoAdd, int(mongoPort))
                self.store = mongo['credentials']
                self.users =  self.store['users']
                self.sources = self.store['sources']
                if self.users.count() == 0:
                    # Default user is "admin" and default password is "admin"
                    self.users.insert({"user": "admin", "password": "8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918"})
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
                mongoAdd, sep, mongoPort = mg.rpartition(":")
                if mongoAdd == '':
                    log.msg('mongo: %s is not a correct address', mg)
                    return 
                mongoAdd = '[%s]' % mongoAdd
                print "conf mongo", mongoAdd, mongoPort
                _connect(mongoAdd, mongoPort,0)
            else:
                self.store = None
                self.users =  None
                self.sources = None
				
        self.conf.get_mongod_all(_call)
   
   

CERT_FILE = "ca.crt"
KEY_FILE = "ca.key"

def create_self_signed_cert(cert_dir):
    """
    If datacard.crt and datacard.key don't exist in cert_dir, create a new
    self-signed cert and keypair and write them into that directory.
    """
    if not exists(join(cert_dir, CERT_FILE)) \
            or not exists(join(cert_dir, KEY_FILE)):
            
        # create a key pair
        k = crypto.PKey()
        k.generate_key(crypto.TYPE_RSA, 1024)

        # CREATE A SELF-SIGNED CERT
        cert = crypto.X509()
        cert.get_subject().C = "FR"
        cert.get_subject().ST = "Paris"
        cert.get_subject().L = "Paris"
        cert.get_subject().O = "Resilient Logger"
        cert.get_subject().OU = "Resilient Logger"
        cert.get_subject().CN = gethostname()
        cert.set_serial_number(1000)
        cert.gmtime_adj_notBefore(0)
        cert.gmtime_adj_notAfter(10*365*24*60*60)
        cert.set_issuer(cert.get_subject())
        cert.set_pubkey(k)
        cert.sign(k, 'sha1')
 
        open( join(cert_dir, CERT_FILE), "wt").write(
            crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
        open( join(cert_dir, KEY_FILE), "wt").write(
            crypto.dump_privatekey(crypto.FILETYPE_PEM, k))


  
def config_ssl(reactor, certDir, factory, host, port):
    create_self_signed_cert(certDir)
    keyfile = join(certDir, KEY_FILE)
    certfile = join(certDir, CERT_FILE )
    print keyfile
    print certfile
    try:
        sslContext = ssl.DefaultOpenSSLContextFactory(keyfile,
                                                  certfile,
                                                 )
        log.msg("SSL certificat configured")
        reactor.listenSSL(port, # integer port                                                                                         
                          factory, # our site object                                                                                   
                          contextFactory = sslContext,
                          interface = host
                          )
    except Exception, e:
        log.msg(e)
        reactor.stop()
        return
    
    
def initServerFactory(producer, zc, reactor, certDir, host, port):
    """
    Initialize the HTTP server
    """     
    cred = CredentialStore( Config(zc))
    indexer = SolrIndexer( Config(zc))
    dbName = 'resilience21'
    storage = MongoGridFs(dbName, Config(zc), reactor)
    root =  RootResource(cred, producer, indexer, storage)
    factory = Site(root)
    config_ssl(reactor, certDir, factory, host, port)
