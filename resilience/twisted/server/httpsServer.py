#!/usr/bin/python
'''
Wallix

@author: Lahoucine BENLAHMR
@contact: lbenlahmr@wallix.com ben.lahoucine@gmail.com
'''
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
from resilience.zookeeper.DataIndexer import SolrIndexer
from OpenSSL import crypto, SSL
from socket import gethostname
from os.path import exists, join
from twisted.internet import ssl

log.startLogging(sys.stdout)

class RootResource(Resource):
    def __init__(self, credentialStore, producer, indexer):
        Resource.__init__(self)
        self.putChild('register', RegisterHandler(credentialStore))
        self.putChild('change', ChangeHandler(credentialStore))
        self.putChild('send', LogCollectHandler(producer, credentialStore))
        self.putChild('search', searchHandler(credentialStore, indexer))
        self.putChild('remove', RemoveHandler(credentialStore))
        
        
class RegisterHandler(Resource):
	
    def __init__(self, credentialStore):
        self.cred = credentialStore
		
    def render_POST(self, request):
        #tester si user et pass sont fournis ...
        user = request.args['user'][0]
        password = request.args['password'][0]
        sourceName = request.args['source'][0]
        print self.cred.checkUser(user, password)
        if self.cred.checkUser(user, password):
            return self.cred.addSource(sourceName)
        return None
        
    def render_GET(self, request):
        request.setResponseCode(http.NOT_FOUND)
        return """
            <html><body>use post method for direct registration or form below:<br><br>
            <form action='/register' method=POST>
            Admin    : <input type='text' name='user'><br>
            Password : <input type='password' name='password'><br>
            Source name to add : <input type='text' name='source'><br>
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
            return self.cred.removeSource(sourceName)
        return None
        
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
        else:
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
        if self.cred.checkUser(user, password):
            return str(self.indexer.search(query))
            
    def render_GET(self, request):
        request.setResponseCode(http.NOT_FOUND)
        return """
            <html><body>use post method for direct search or form below:<br><br>
            <form action='/search' method=POST>
            Admin    : <input type='text' name='user'><br>
            Password : <input type='password' name='password'><br>
            query : <input type='text' name='query'><br>
            <input type='submit'>
            </body></html>      
        """
        
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
            request.setResponseCode(LogCollect.NO) # send code response NO
            return ""
        key = postpath[0]
        log.msg( "session: %s"% request.getSession().uid)
        #startTime = time()
        sourceName = self.cred.checkSource(key)
        if sourceName:
            code = logProcess(request, self.prod, sourceName)
            if code:
                request.setResponseCode(int(code))
            else:
                request.setResponseCode(LogCollect.UNAVAILABLE)                
            try:	
                request.finish()
            except:
		pass
	else:
            request.setResponseCode(LogCollect.NO)
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
        return LogCollect.OK
       

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

    def addSource(self, name):
        key = self._key_generator()
        exists = self.sources.find_one({"name": name}, {"name": 1})
        print exists
        if not exists :
            try :
                source = { "name" : name, "key": key }
                self.sources.insert(source)
                return key
            except:
                pass				
        return None
    
    def removeSource(self, name):
        try:
            source = {"name" : name}
            self.sources.remove(source)
        except:
            pass
		
    def checkSource(self, key):
        source = self.sources.find_one({"key": key})
        print "check source", key
        if source:
            print source
            return source["name"]
        return None
	
    def checkUser(self, user, password):
        sha = SHA256.new(password)
        exists = self.users.find_one({"user": user, "password": sha.hexdigest()})
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
    root =  RootResource(cred, producer, indexer)
    factory = Site(root)
    config_ssl(reactor, certDir, factory, host, port)