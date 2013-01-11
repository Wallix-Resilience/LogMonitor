'''
Created on 9 janv. 2013

@author: lahoucine
'''
import argparse
import sys
from txzookeeper.client import ZookeeperClient
from txzookeeper.retry import RetryClient
import zookeeper
from twisted.internet import reactor, defer
from resilience.zookeeper.configure.config import Config
from compiler.ast import Add

def get_zk(server, callback):
    zc = RetryClient(ZookeeperClient(server))
    d = zc.connect()
    d.addCallback(callback, zc)
    reactor.run()
    return d

def add_key(server, key):
    
    def cb_connected(self, zc):
        cfg = Config(zc)
        cfg.add_key_ca(key)
        reactor.stop()
        
    #add key
    if len(key):
        print key
    get_zk(server, cb_connected)
    
    
def add_certificat(server, certificat):
    
    def cb_connected(self, zc):
        cfg = Config(zc)
        cfg.add_certificat_ca(certificat)
        reactor.stop()
    
    #add certificat
    if len(certificat):
        print certificat
    get_zk(server, cb_connected)

def cmd(server, command):
    usage = "usage"

    if command == "addCA":
        add_key(server)
        add_certificat(server)
        
    elif command == "addCert":
        add_certificat(server)
     
    elif command == "addKey":
        add_key(server)
    else:
        print usage
    
def main():
    params = sys.argv[1:]
    parser = argparse.ArgumentParser(description='Resilient Log configuration client')
    
    parser.add_argument('-s','--server',help='address of the zookeeper server', 
                                          default="localhost:2181", required=False)
    parser.add_argument('-c', '--addCertificat', help= 'add certificat for producers', required = False )
    parser.add_argument('-k', '--addKey', help= 'add key for producers', required = False )
    
    args = parser.parse_args(params)
    server = args.server 
    certificat = args.addCertificat
    key = args.addKey
    
    if certificat:
        add_certificat(server, certificat)
    
    if key:
        add_key(server, key)
        
if __name__ == '__main__':
    main()
        
         
   
    

    