#!/usr/bin/python
'''
Wallix

@author: Lahoucine BENLAHMR
@contact: lbenlahmr@wallix.com ben.lahoucine@gmail.com
'''
from txzookeeper.client import ZookeeperClient
from txzookeeper.retry import RetryClient
import zookeeper
import sys
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto import Random
import os
from twisted.python import log
from twisted.internet import reactor, defer

log.startLogging(sys.stdout)


class Config():
    """
    The aim of this class is to centralize and manage  most configurations
    using  Zookeeper
    """
        
    def __init__(self, zkAddr):
        """Initialization of a configuration manager.
        create a configuration tree into zookeeper
        @param zkAddr: Zookeeper client instance.
        """
        self.zk = zkAddr
        d = self.zk.create("/producers")
        d.addErrback(self._err)
        d = self.zk.create("/consumer")
        d.addErrback(self._err)
        d = self.zk.create("/mongo")
        d.addErrback(self._err)
        d = self.zk.create("/solr")
        d.addErrback(self._err)
        d = self.zk.create("/ssl")
        d.addErrback(self._err)
        d = self.zk.create("/ssl/ca")
        d.addErrback(self._err)
        d = self.zk.create("/nodes")
        d.addErrback(self._err)
        
    def _err(self,error):
        """
        error handler
        @param error: error message
        """
        log.msg('node seems to already exists : %s' % error)
        
    def add_key_ca(self, key):
        """
        Add the key file of the ca into the configuration tree
        @param key: path to the key file 
        """
        f = open(key)
        d = self.zk.create("/ssl/ca/key",f.read())
        d.addErrback(self._err)
        
    def add_certificat_ca(self, cert=None):
        """
        Add the certificate  file of the ca into the configuration tree
        @param cert: path to certificate file 
        """
        f = open(cert)
        d = self.zk.create("/ssl/ca/certificat",f.read())
        d.addErrback(self._err)
            
    def get_key_ca(self, path=None):
        """
        get ca key from configuration tree
        @param path: to location where to store the key 
        """
        def _call(keydata, path):
            if path == None:
                path = "."
            path = os.path.abspath(path)
            keyfile = open(os.path.join(path, "ca.key"),"w")
            keyfile.write(keydata[0])
            keyfile.close()
            
        self._get_data("/ssl/ca/key",lambda x:_call(x, path))
        
      
    def get_certificat_ca(self, path=None):
        """
        get ca certificate from the configuration tree
        @param path: the location where to store the certificate
        """
        def _call(keydata, path):
            if path == None:
                path = "."
            path = os.path.abspath(path)
            keyfile = open(os.path.join(path, "ca.cert"),"w")
            keyfile.write(keydata[0])
            keyfile.close()
            
        self._get_data("/ssl/ca/certificat",lambda x:_call(x, path))
    

    
    def add_node(self, name):
        """
        add a new collection source into the configuration tree and generate
        a key for it.
        @param name: name of collection source 
        """
        def _key_generator():
            """
            generate a key, by hashing an RSA key generated randomly.
            the hashing method used is sha2
            """
            random_generator = Random.new().read
            key = RSA.generate(1024, random_generator)
            exportedKey = key.exportKey()
            sha = SHA256.new()
            sha.update(exportedKey)
            k = sha.hexdigest()
            print "keep your key secret: %s" % k
            return k
        
        hash = _key_generator()
        print "node key:", hash
        d = self.zk.create("/nodes/%s" % hash, name)
        d.addErrback(self._err)
        
    def get_node(self, hash, callback, errback):
        """
        get the collection source name, giving the key
        @param hash: the hash key of the collection source 
        @param callback: a function to call if the key exist 
                         in the configuration tree 
        @param errback: a function to call if the key don't exist 
                        in the configuration tree
        """
        nodepath = "/nodes/%s" % hash
        self._get_data(nodepath, callback, errback)

    def add_producer(self,producer):
        """
        add a producer address in the configuration tree
        @param producer: producer address
        """
        d = self.zk.create("/producers/%s" % str(producer), flags = zookeeper.EPHEMERAL)
        d.addErrback(self._err)
              
#    def add_mongod(self, mongod):
#        d = self.zk.create("/monggodb/mongod/%s" % str(mongod))
#        d.addErrback(self._err)
#                        
#    def add_solr(self, solr):
#        d =  self.zk.create("/solr/%s" % str(solr))
#        d.addErrback(self._err)
    
    def get_mongod_all(self, callback = None):
        """
        get all mongoDB addresses from configuration tree
        @param callback: function to call after getting data 
        from the configuration tree
        """
        self._get_conf_all("/mongo", callback)
                   
    def get_solr_all(self, callback = None):
        """
        get all solr addresses from configuration tree
        @param callback: call this function after getting data 
        from the configuration tree
        """
        self._get_conf_all("/solr", callback)
    
    def _get_conf_all(self, path, callback = None):
        """
        get all configurations from the configuration tree, using a giving path
        @param path: path of the configuration to get
        @param callback: the function to call after getting the configuration  
        """
        
        def _get_value(m):
            value = m
            if callback:
                callback(value)   
        def _call(m):
            dat, m = self.zk.get_children_and_watch(path)
            dat.addCallback(_get_value)
            m.addCallback(_call)
            m.addErrback(self._err)

        if not callback:
            data = self.zk.get(path)
            data.addCallback(_get_value)
            data.addErrback(self._err)

        else:
            data, diff = self.zk.get_children_and_watch(path)
            data.addCallback(_get_value)
            data.addErrback(self._err)
            diff.addCallback(_call)
            diff.addErrback(self._err)

    def _get_conf_data(self, path, callback = None):
        """
        get and watch data from the configuration tree, using a giving path
        @param path: path to the configuration to get
        @param callback: the function to call after getting the configuration  
        """
        def _get_value(m):
            value = m
            if callback:
                callback(value)   
                
        def _call(m):
            dat, m = self.zk.get_and_watch(path)
            dat.addCallback(_get_value)
            m.addCallback(_call)
            m.addErrback(self._err)

        if not callback:
            data = self.zk.get(path)
            data.addCallback(_get_value)
            data.addErrback(self._err)
       
        else:
            data, diff = self.zk.get_and_watch(path)
            data.addCallback(_get_value)
            data.addErrback(self._err)
            diff.addCallback(_call)
            diff.addErrback(self._err)

    def _get_data(self, path, callback = None, errback = None):
        """
        get data from the configuration tree without watch.
        @param path: path to the configuration to get
        @param callback: function to call after getting data
        @param errback: function to call if getting data fails         
        """
        data = self.zk.get(path)
        if callback:
            data.addCallback(callback)
        if errback:
            data.addErrback(errback)
            
           
            
if __name__ == "__main__":
    def cb_connected(self, zc):
        cfg = Config(zc)
        def _call(m):
            print "call",m[0]
        #cfg._get_conf_all("/log_chunk_produced42",_call)
        
        cfg.add_key_ca('../../../ssl/ca/privkey.pem')
        cfg.add_certificat_ca('../../../ssl/ca/cacert.pem')
        #cfg.add_node("dddkdkklqsdmqmdlqmdq", "toto")
        
        # cfg._get_conf_data("/configs/myconf/schema.xml",_call)
        #cfg._get_conf_all("/producers",_call)
        #cfg.get_solr_all(_call)
        #cfg.get_mongod_all(_call)
        #cfg.add_solr('localhost6:20120')
        #cfg.add_node("lahoucine")
        #cfg._get_data("/nodes/b262d217d3a2faa167d9f9f2a182623405e65dc7632a1ce48840659b7e1de8d",_call)
        cfg.get_key_ca()
        cfg.get_certificat_ca()
    zc = RetryClient(ZookeeperClient("2001:470:1f14:169:c00d:4cff:fed4:4894:29017"))
    d = zc.connect()
    d.addCallback(cb_connected, zc)
    d.addErrback(log.msg)
    reactor.run()

    
    

