#!/usr/bin/python

import sys
import os
import random
import time
import uuid
from twisted.python import log
from twisted.internet import reactor, defer
from txzookeeper.client import ZookeeperClient
from txzookeeper.retry import RetryClient
from txzookeeper.queue import ReliableQueue

log.startLogging(sys.stdout)

class LogProducer():

    def __init__(self, datadir, znode_path, zcrq):
        self.datadir = datadir
        self.znode_path = znode_path
        self.zcrq = zcrq
        self.p = 0

    def produce_many(self):
        def _produce():
            self.produce()
            self.p += 1
            if self.p < 1000000:
                reactor.callLater(1, _produce)
        reactor.callLater(1, _produce)

    def produce(self):
        filename = 'log%s_%s.log' % (str(uuid.uuid4()), int(time.time()))
        filepath = os.path.join(self.datadir, filename)
        f = file(filepath, 'w')
        f.write('a log\n')
        f.close()
        log.msg('Log chunk write on %s' % filepath)
        self.publish(filepath)
        
    def publish(self, filepath):
        d = self.zcrq.put(filepath)
        d.addCallback(lambda x: log.msg('Log chunk published on queue : %s' % x))
        d.addErrback(lambda x: log.msg('Unable to publish chunk on queue : %s' % x))

def cb_connected(useless, zc, datadir):
    def _err(error):
        log.msg('Queue znode seems to already exists : %s' %error)
    znode_path = '/log_chunk_produced3'
    d = zc.create(znode_path)
    d.addCallback(lambda x: log.msg('Queue znode created at %s' % znode_path))
    d.addErrback(_err)
    zcrq = ReliableQueue(znode_path, zc, persistent = True)
    lp = LogProducer(datadir, znode_path, zcrq)
    lp.produce_many()
    
if __name__ == "__main__":
    datadir = '/tmp/rawdata'
    if not os.path.isdir(datadir):
        os.mkdir(datadir)
    zc = RetryClient(ZookeeperClient("127.0.0.1:2181"))
    d = zc.connect()
    d.addCallback(cb_connected, zc, datadir)
    d.addErrback(log.msg)
    reactor.run()
