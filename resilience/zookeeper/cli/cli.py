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

def get_zk(server, callback):
    zc = RetryClient(ZookeeperClient(server))
    d = zc.connect()
    d.addCallback(callback, zc)
    reactor.run()
    return d

def add_key(server):
    
    def cb_connected(self, zc):
        cfg = Config(zc)
        cfg.add_key_ca(key)
        reactor.stop()
        
    #add key
    sys.stdout.write('Producers key:')
    key = sys.stdin.readline().strip()
    if len(key):
        print key
    get_zk("fd88:9fde:bd6e:f57a:0:1d83:ed10:4574:29017", cb_connected)
    
    
def add_certificat(server):
    
    def cb_connected(self, zc):
        cfg = Config(zc)
        cfg.add_certificat_ca(key)
        reactor.stop()
    
    #add certificat
    sys.stdout.write('Producers certificat: ')
    certificat = sys.stdin.readline().strip()
    if len(certificat):
        print certificat
    get_zk("fd88:9fde:bd6e:f57a:0:1d83:ed10:4574:29017", cb_connected)

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
    parser.add_argument('-c', '--command', help= 'command to execute', required = True )
    
    args = parser.parse_args(params)
    server = args.server 
    command = args.command
    cmd(server, command)
    
if __name__ == '__main__':
    
    

    