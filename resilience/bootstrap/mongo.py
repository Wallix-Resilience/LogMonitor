#!/usr/bin/python
# coding=utf-8
from txzookeeper.client import ZookeeperClient
from txzookeeper.retry import RetryClient
import zookeeper
import subprocess
from twisted.internet import reactor, defer, ssl
from twisted.python import log
import sys
import argparse
import signal
from pymongo import Connection
from pymongo.errors import AutoReconnect
from pymongo.errors import OperationFailure
from time import sleep

log.startLogging(sys.stdout)


def addshards():
    params = sys.argv[1:]    
    parser = argparse.ArgumentParser(description='add shards into mongodb cluster')
    parser.add_argument('-s','--shards',help='list of shards', nargs='+', required=True)
    parser.add_argument('-i','--bind_ip',help='mongos binding ip', required=True)
    parser.add_argument('-p','--port',help='mongos listening port', required=True)
    args = parser.parse_args(params)

    shards = args.shards
    shards = shards[0].split()
    host = args.bind_ip
    port =  int(args.port)
    
    sleep(10)
    while (True):
        try:
            conn = Connection(host, port)
            for shard in shards:
                try:
                    print "Adding shard: %s" % shard
                    conn.admin.command("addshard", shard)
                except OperationFailure, e:
                    print e
            break
        except AutoReconnect:
            sleep(1)

    print "*** READY ***"
    print
    if reactor.running:
        reactor.stop()
    
def initsharding():    
    params = sys.argv[1:]    
    parser = argparse.ArgumentParser(description='init shard replication in a givin node')
    parser.add_argument('-s','--shard',help='the shard\'s name', required=True)
    parser.add_argument('-n','--nodes',help=' nodes composing the shard', nargs='+', default = "", required=True)
    parser.add_argument('-i','--bind_ip',help='binding ip', required=True)
    parser.add_argument('-p','--port',help='listening port', required=True)
    args = parser.parse_args(params)
    
    shardname = args.shard
    nodes = args.nodes
    nodes = nodes[0].split()
    host = args.bind_ip
    port =  int(args.port)
    
    config = {"_id": shardname,
          "members": []}
    for i in range(len(nodes)):
        if nodes[i] == ":":
            sys.exit()
        member = {"_id": i, "host": nodes[i]}
        config["members"].append(member)
        
    sleep(10)
    while (True):
        try:
            conn = Connection(host, port)
            try:
                conn.admin.command("replSetInitiate", config)
                print conn.admin.command("replSetGetStatus")
            except OperationFailure, e:
                print "mongodb: ", e
                break
            print "*** READY ***"
            print
            break
        except AutoReconnect:
            print "Mongodb can't connect to  %s in %s" % (str(port), host)
            print "Trying again!"
            sleep(1)
            
    if reactor.running:
        reactor.stop()
    


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
    # penser à enlever l'option --rest pour mongos
    arguments = [mongod,
                 "--bind_ip", ip,
                 "--port", port,
                 "--ipv6", 
                 "--config", conf 
                 ]
    print "args:", arguments
    try:
        p = subprocess.Popen(arguments)
        #subprocess.call(arguments)
    except:
        if reactor.running:
            reactor.stop()
        
    signal.signal(signal.SIGTERM, handler)
    p.wait()
    
def handler(signum, frame):
    global p
    "terminate process %s ..." % p.pid
    try:
        p.terminate()
        if reactor.running:
            reactor.stop()
    except:
        print "No such process or not killed!"
    
def main():
    """
    the aim of this code is to start an instance of mongo, and publishing
    its configuration (address + port) into the configuration tree managed
    by Zookeeper.
    the znode created for this configuration must be an Ephemeral.
    with that type of znode, the configuration added exist only if the client
    is up.
    """
    params = sys.argv[1:]
        
    parser = argparse.ArgumentParser(description='mongo bootstrap')
    parser.add_argument('-z','--zkaddr',help='zookeeper address', required=True)
    parser.add_argument('-b','--bin',help='mongodb bin', required=True)
    parser.add_argument('-c','--config',help=' path to configuration file of mongodb', default = "", required=False)
    parser.add_argument('-i','--bind_ip',help='bind ip', required=True)
    parser.add_argument('-p','--port',help='listen port', required=True)

    args = parser.parse_args(params)
    zk = args.zkaddr
    mongod = args.bin
    conf = args.config
    ip = args.bind_ip
    port = args.port
    try:    
        zc = RetryClient(ZookeeperClient(zk))
        d = zc.connect()
        d.addCallback(cb_connected, zc, mongod, ip, port, conf)
        d.addErrback(log.msg)
    except:
        print "Can't connect to zookeeper!"
        return
    reactor.run()

if __name__ == "__main__":
    main()
