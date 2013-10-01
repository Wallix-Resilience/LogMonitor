from twisted.trial.unittest import TestCase
from txzookeeper.retry import RetryClient
import resilience.zookeeper.producersConsumers.producerNg as producer
import resilience.zookeeper.producersConsumers.consumerNg as consumer
from txzookeeper.queue import ReliableQueue
from resilience.zookeeper.configure.config import Config
from twisted.internet.defer import (
    inlineCallbacks, returnValue, DeferredList, Deferred, succeed, fail)
from txzookeeper import ZookeeperClient
from resilience.data.DataStorage import MongoGridFs
from resilience.data.DataIndexer import SolrIndexer
import zookeeper


class ProducerConsumerTest(TestCase):
    
    def setUp(self):
        super(ProducerConsumerTest, self).setUp()
        self.clients = []
        self.znode_path = '/log_chunk_test'
        self.storage = None
        self.indexer = None
        self.normalizer = '/home/lahoucine/git/pylogsparser/normalizers'
        
    def tearDown(self):
        cleanup = False
        for client in self.clients:
            if not cleanup and client.connected:
                self.deleteTree(path=self.znode_path, handle=client.handle)
                cleanup = True
            if client.connected:
                client.close()
        if self.indexer:
            self.indexer.delete_by_query("*:*")
        super(ProducerConsumerTest, self).tearDown()
        
    
    
    def deleteTree(self, path="/", handle=1):
        """
        Destroy all the nodes in zookeeper path
        """
        for child in zookeeper.get_children(handle, path):
            if child == "zookeeper":  # skip the metadata node
                continue
            child_path = "/" + ("%s/%s" % (path, child)).strip("/")
            try:
                self.deleteTree(child_path, handle)
                zookeeper.delete(handle, child_path, -1)
            except zookeeper.ZooKeeperException, e:
                print "Error on path", child_path, e
            
        
    @inlineCallbacks
    def open_client(self, credentials=None):
        """
        Open a zookeeper client, optionally authenticating with the
        credentials if given.
        """
        client = ZookeeperClient("127.0.0.1:2181")
        self.clients.append(client)
        yield client.connect()
        if credentials:
            d = client.add_auth("digest", credentials)
            # hack to keep auth fast
            yield client.exists("/")
            yield d
        returnValue(client)
    
    def sleep(self, delay):
        """Non-blocking sleep."""
        from twisted.internet import reactor
        deferred = Deferred()
        reactor.callLater(delay, deferred.callback, None)
        return deferred
        
    @inlineCallbacks
    def initQueue(self):
        zc =  yield self.open_client()
        #import pdb; pdb.set_trace()
        path = self.znode_path
        try:
            path = yield zc.create(self.znode_path)
        except:
            pass
        queue =  yield ReliableQueue(path, zc, persistent = True)
        returnValue(queue)
        
    @inlineCallbacks
    def getStorage(self):
        from twisted.internet import reactor
        zc = yield self.open_client()
        storage =  yield MongoGridFs("resilience_test", Config(zc), reactor)
        returnValue(storage)
    
    @inlineCallbacks
    def getIndexer(self):
        zc = yield self.open_client()
        indexer = yield SolrIndexer(Config(zc))
        returnValue(indexer)
        
    @inlineCallbacks
    def create_producer(self):
        #create the reliable queue
        queue = yield self.initQueue()
        #get storage
        self.storage = yield self.getStorage()
        #connect to zookeeper
        zc = yield self.open_client()
        producer.MAX_LINE = 1
        lproducer = yield producer.LogProducer( self.znode_path, zc, 
                                                  queue, self.storage)
        returnValue(lproducer)
        
    @inlineCallbacks
    def create_consumer(self):
        #create the reliable queue
        queue = yield self.initQueue()
        #get storage
        self.storage = yield self.getStorage()
        #get indexor
        self.indexer = yield self.getIndexer()
        #connect to zookeeper
        zc = yield self.open_client()
        lconsumer = yield consumer.LogConsumer(self.znode_path, zc, 
                     queue, self.storage, self.indexer, self.normalizer)
        returnValue(lconsumer)
        
    @inlineCallbacks
    def test_producer_put_log_in_queue_and_storage(self):
        #get an instance of producer
        lproducer = yield self.create_producer()
        #process a log line by the producer
        source_name = 'mach1'
        logLine = 'Sep 26 15:31:39 lahoucine-HP kernel: [188510.991597] vboxnetflt: dropped 15472 out of 24223 packets'
        yield self.sleep(0.5)
        yield lproducer.produce(logLine, source_name)
        #teste if we have the same log in storage after process
        #get the file path
        queue = yield self.initQueue()
        data = yield queue.get()
        #get the content of file from database
        filed = yield self.storage.getFile(data)
        line = filed.readline()
        filed.close()
        self.assertEqual(logLine.strip() , line.strip())

    @inlineCallbacks
    def test_consumer_get_log_from_queue(self):
        #get an instance of consumer
        lconsumer = yield self.create_consumer()
        #get an instance of producer
        lproducer = yield self.create_producer()
        yield self.sleep(0.5)
        #produce a log line
        source_name = 'mach1'
        logLine = 'Sep 26 15:31:39 lahoucine-HP kernel: [188510.991597] vboxnetflt: dropped 15472 out of 24223 packets'
        yield lproducer.produce(logLine, source_name)
        #consume a log line
        yield lconsumer.consume()
        yield self.sleep(0.5)
        yield lconsumer._indexCommiter()
        #test if the log was consumed and indexed
        #wait the log to be indexed
        yield self.sleep(0.5)
        indexed_log =  self.indexer.search(query="*:*")[0]['raw']
        print indexed_log
        self.assertEqual(logLine, indexed_log)
        
    @inlineCallbacks
    def test_queue_size_before_consuming(self):
        #get an instance of producer
        lproducer = yield self.create_producer()
        #process a log line by the producer
        source_name = 'mach1'
        logLine = 'Sep 26 15:31:39 lahoucine-HP kernel: [188510.991597] vboxnetflt: dropped 15472 out of 24223 packets'
        yield self.sleep(0.5)
        yield lproducer.produce(logLine, source_name)
        queue = yield self.initQueue()
        queue_size = yield queue.qsize()
        print "sizeeeee", queue_size
        self.assertEqual(queue_size, 1)
        
    @inlineCallbacks
    def test_queue_size_afte_consuming(self):
        #get an instance of consumer
        lconsumer = yield self.create_consumer()
        #get an instance of producer
        lproducer = yield self.create_producer()
        yield self.sleep(0.5)
        #produce a log line
        source_name = 'mach1'
        logLine = 'Sep 26 15:31:39 lahoucine-HP kernel: [188510.991597] vboxnetflt: dropped 15472 out of 24223 packets'
        yield lproducer.produce(logLine, source_name)
        #consume a log line
        yield lconsumer.consume()
        yield self.sleep(0.5)
        yield lconsumer._indexCommiter()
        #test if the log was consumed and indexed
        #wait the log to be indexed
        yield self.sleep(0.5)
        queue = yield self.initQueue()
        queue_size = yield queue.qsize()
        self.assertEqual(queue_size, 0)
