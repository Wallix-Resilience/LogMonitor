import sys
from OpenSSL import SSL
from twisted.python.log import err
from twisted.web.client import Agent
from twisted.internet import reactor
from twisted.internet.ssl import ClientContextFactory
from twisted.internet.defer import succeed
from twisted.web.iweb import IBodyProducer
from zope.interface import implements
from twisted.python import log


class StringProducer(object):
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return succeed(None)

    def pauseProducing(self):
        print "pause"
        pass

    def stopProducing(self):
        print "stop"
        pass

class WebClientContextFactory(ClientContextFactory):
    def __init__(self, cert, key):
        self.cert = cert
        self.key = key
    def getContext(self, hostname, port):
        self.method = SSL.SSLv23_METHOD
        ctx = ClientContextFactory.getContext(self)
        ctx.use_certificate_file(self.cert)
        ctx.use_privatekey_file(self.key)
        return ctx
    
    
class httpRequest(object):
    
    def __init__(self, server, cert, key):
        self.server = server
        self.cert = cert
        self.key = key
        
       
    def httpPostRequest(self,data):
        # Construct an Agent.
        contextFactory = WebClientContextFactory(self.cert,self.key)
        agent = Agent(reactor, contextFactory)
        #data = urllib.urlencode(values)
    
        d = agent.request('POST',
                          self.server,
                          bodyProducer=StringProducer(data))
        
        def handle_response(response):
            print "response received"
            #reactor.stop()
    
        def _err(error):
            log.msg('Error : %s' %error)
#            try:
#                #reactor.stop()
#            except:
#                log.msg("server already stopped")
#    
        def cbResponse(ignored):
    		print 'Response received'
        d.addCallback(cbResponse)
        
        def cbShutdown(ignored):
            reactor.stop()
            
        d.addBoth(cbShutdown)
        d.addErrback(_err)
        reactor.run()
    
    
def main():
    http = httpRequest('https://localhost:8990',
                       '/home/lahoucine/certs/ss_cert_c.pem',
                       '/home/lahoucine/keys/ss_key_c.pem')
    http.httpPostRequest("hhfsf h")    
    
if __name__ == "__main__":
    main()