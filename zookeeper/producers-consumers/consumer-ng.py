#!/usr/bin/python

import sys
import os
import random
import time
from twisted.python import log
from twisted.internet import reactor, defer
from txzookeeper.client import ZookeeperClient
from txzookeeper.retry import RetryClient
from txzookeeper.queue import ReliableQueue

log.startLogging(sys.stdout)

class LogConsumer():

    def __init__(self, datadir, znode_path, zcrq):
        self.datadir = datadir
        self.znode_path = znode_path
        self.zcrq = zcrq
        self.p = 0

    def consume_many(self):
        def _consume():
            self.consume()
            self.p += 1
            if self.p < 1000000:
                reactor.callLater(1, _consume)
        reactor.callLater(1, _consume)

    def consume(self):
        def _consuming(item):
            log.msg('Consuming %s' % item.data)
            try:
                os.unlink(item.data)
                log.msg('Remove %s from log chunk path.' % item.data)
                item.delete()
            except Exception, e:
                log.msg('WARNING unable to suppress %s due to : %s' % (item.data, e))
        d = self.zcrq.get()
        d.addCallback(_consuming)

def cb_connected(useless, zc, datadir):
    def _err(error):
        log.msg('Queue znode seems to already exists : %s' %error)
    znode_path = '/log_chunk_produced3'
    zcrq = ReliableQueue(znode_path, zc, persistent = True)
    lc = LogConsumer(datadir, znode_path, zcrq)
    lc.consume_many()
    #lc.consume()
    
if __name__ == "__main__":
    datadir = '/tmp/rawdata'
    if not os.path.isdir(datadir):
        os.mkdir(datadir)
    zc = RetryClient(ZookeeperClient("127.0.0.1:2181"))
    d = zc.connect()
    d.addCallback(cb_connected, zc, datadir)
    d.addErrback(log.msg)
    reactor.run()
