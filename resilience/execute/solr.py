#!/usr/bin/python
'''
Wallix

@author: Lahoucine BENLAHMR
@contact: lbenlahmr@wallix.com ben.lahoucine@gmail.com
'''
from txzookeeper.client import ZookeeperClient
from txzookeeper.retry import RetryClient
import zookeeper
import subprocess
from twisted.internet import reactor, defer, ssl
from twisted.python import log
import sys
import os
import argparse
import signal

log.startLogging(sys.stdout)

global p
def cb_connected(self, zc, cores, solrpath, solrhome, numsh, port, ip, java, zk):
    global p
    
    d = zc.create("/solr")
    def _err(error):
        log.msg('node seems to already exists : %s' % error)
    solr = "%s:%s" %(ip,port)
    d = zc.create("/solr/%s" % str(solr),flags = zookeeper.EPHEMERAL)
    d.addErrback(_err)
    
    cores = cores.strip()
    solrpath = solrpath.strip()
    port = port.strip()
    ip = ip.strip()
    java = java.strip()
    zk = zk.strip()
    solrhome = solrhome.strip()
    numsh = numsh.strip()
    
    confname = '-Dcollection.configName=confResilience'
    solrh = '-Dsolr.solr.home=%s' % solrhome
    confdir = '-Dbootstrap_confdir=%s' % cores # modifier
    jettyPath =  '-Djetty.home=%s' % solrpath
    jettyPort = '-Djetty.port=%s' % port
    jettyHost = '-Djetty.host=%s' % ip
    host = '-Dhost=[%s]' % ip 
    zkrun = '-DzkRun'
    zkhost = '-DzkHost=%s' % zk
    numShard = numsh
    start = os.path.join(solrpath, "start.jar")

    arguments = [java,"-Xmx2048m", host, jettyPath, confdir, solrh, numShard, confname, jettyHost, jettyPort, zkhost, "-DnumShards=","-jar", start]
    print arguments
    #subprocess.check_output(arguments)
    p = subprocess.Popen(arguments)
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGKILL, handler)
    p.wait()

def handler(signum, frame):
    global p
    "terminate process %s ..." % p.pid
    p.terminate()
    
      
def main():
    """
    the aim of this code is to start an instance of solr, and publishing
    its configuration (address + port) into the configuration tree managed
    by Zookeeper.
    the znode created for this configuration must be a Ephemeral.
    with that type of znode, the configuration added exist only if the client
    is up.
    """
    params = sys.argv[1:]
        
    parser = argparse.ArgumentParser(description='Solr bootstrap')
    parser.add_argument('-z', '--zkaddr', help='zookeeper address', required=True)
    parser.add_argument('-c', '--cores', help='cores path', required=True) #TO DO a remplacer par conf
    parser.add_argument('-m', '--solrhome', help='solr home', required=True) #TO DO a remplacer par conf
    parser.add_argument('-s', '--solrpath', help='solr path', required=True)
    parser.add_argument('-p','--port', help='listening port', required=True)
    parser.add_argument('-i','--ip', help='listening ip', required=True)
    parser.add_argument('-j','--java',help='java home', required=True)
    parser.add_argument('-n','--numshard',help='number shard', required=True)

    args = parser.parse_args(params)
    
    zk = args.zkaddr
    cores = args.cores
    solrpath = args.solrpath
    solrhome = args.solrhome
    port = args.port
    ip = args.ip
    java = args.java
    numsh = args.numshard
        
    zc = RetryClient(ZookeeperClient(zk))
    d = zc.connect()
    d.addCallback(cb_connected, zc, cores, solrpath, solrhome, numsh, port, ip, java, zk)
    d.addErrback(log.msg)
    reactor.run()

if __name__ == "__main__":
    main()
