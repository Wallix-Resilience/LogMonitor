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
import argparse
import signal

log.startLogging(sys.stdout)
global p
def cb_connected(self, zc, mongod, ip, port, conf):
    global p
    
    mongod = mongod.strip()
    ip = ip.strip()
    port = port.strip()
    conf = conf.strip()
    

    d = zc.create("/mongo")
            
    def _err(error):
        log.msg('node seems to already exists : %s' % error)
    mongo = "%s:%s" % (ip, port)
    d = zc.create("/mongo/%s" % str(mongo),flags = zookeeper.EPHEMERAL)
    d.addErrback(_err)
    
    arguments = [mongod,
                 "--bind_ip", ip,
                 "--port", port,
                 "--rest", "--ipv6",
                 "--config", conf 
                 ]
    print "args:", arguments
    try:
        p = subprocess.Popen(arguments)
        #subprocess.call(arguments)
    except:
        reactor.stop()
        
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGKILL, handler)
    p.wait()
    
def handler(signum, frame):
    global p
    "terminate process %s ..." % p.pid
    p.terminate()
    reactor.stop()
    
def main():
    """
    the aim of this code is to start an instance of mongo, and publishing
    its configuration (address + port) into the configuration tree managed
    by Zookeeper.
    the znode created for this configuration must be a Ephemeral.
    with that type of znode, the configuration added exist only if the client
    is up.
    """
    params = sys.argv[1:]
        
    parser = argparse.ArgumentParser(description='mongo bootstrap')
    parser.add_argument('-z','--zkaddr',help='zookeeper address', required=True)
    parser.add_argument('-b','--bin',help='mongodb bin', required=True)
    parser.add_argument('-c','--config',help=' path to configuration file of mongodb', 
                                                        default = "", required=False)
    parser.add_argument('-i','--bind_ip',help='bind ip', required=True)
    parser.add_argument('-p','--port',help='listen port', required=True)

    args = parser.parse_args(params)
    
    zk = args.zkaddr
    mongod = args.bin
    conf = args.config
    ip = args.bind_ip
    port = args.port
        
    zc = RetryClient(ZookeeperClient(zk))
    d = zc.connect()
    d.addCallback(cb_connected, zc, mongod, ip, port, conf)
    d.addErrback(log.msg)
    reactor.run()

if __name__ == "__main__":
    main()