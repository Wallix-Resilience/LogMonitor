#!/usr/bin/python
'''
Wallix

@author: Lahoucine BENLAHMR
@contact: lbenlahmr@wallix.com ben.lahoucine@gmail.com
'''
import sys
from twisted.web.server import Site
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.internet import reactor
from twisted.python import log
from time import *
from resilience.zookeeper.configure.config import Config


log.startLogging(sys.stdout)


class LogCollect(Resource):
    """
    This class represent an HTTP server
    """
    
    OK = 200
    NO = 500
    UNAVAILABLE = 503
    
    isLeaf = True
    def __init__(self,producer, zc):
        """
        Initialization of the HTTP SERVER
        """
        self.prod = producer   
        self.conf = Config(zc) 

     
    def render_POST(self, request):
        """
        process the HTTP POST requests an send a response code to the client:
        200 -> ok
        500 -> no
        503 -> Service Unavailable
        """
        
        def _callback(key):
            """
            called after getting the data key from configuration tree
            @param key: data key
            """
            name = key[0]
            code = logProcess(request, self.prod, name)
            if code:
                request.setResponseCode(int(code))
            else:
                request.setResponseCode(LogCollect.UNAVAILABLE)
            try:
                request.finish()
            except:
                pass
                
        def _errback(key):
            """
            called when failed to get a key 
            """
            request.setResponseCode(LogCollect.NO)
            try:
                request.finish()
            except:
                pass
        
        postpath =  request.postpath # postpath contain the key of a client
        print postpath
        print request.path
        if len(postpath)!= 1 or postpath[0] == '': # if no giving key (postpath)
            request.setResponseCode(LogCollect.NO) # send code response NO
            return ""
        
        key = postpath[0]
        log.msg( "session: %s"% request.getSession().uid)
        #startTime = time()
        self.conf.get_node(key, _callback, _errback)
        
        return NOT_DONE_YET  # we have to wait the response code      
    
   
def initServerFactory(producer, zc):
    """
    Initialize the HTTP server
    """  
    root = LogCollect(producer, zc)
    factory = Site(root)
    return factory    



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
