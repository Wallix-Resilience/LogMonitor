#!/usr/bin/env python

import httplib2
from resilience.twisted.agent.logWatch import LogWatcher
#
from twisted.internet import ssl, reactor
from twisted.internet.protocol import ClientFactory, Protocol
from twisted.python import log

SERVER_ADDR = "https://localhost:8990"

class logAgent():
    def __init__(self, directory, server, AgentKey, AgentCertificat):
        self.server = server
        self.http = httplib2.Http(disable_ssl_certificate_validation=True)
        self.http.add_certificate(AgentKey, AgentCertificat, '')
        self.lw = LogWatcher(directory,self.sendLine)
        
    
    def run(self):
        self.lw.loop()
        
    def sendLine(self,filename,lines):
         for line in lines:
            if  line.strip():
                print "line: " ,line
                resp, content = self.http.request(self.server,
                                                  'POST',
                                                  line,
                                                  headers={'Content-Type': 'text/plain'}
                                                  )
                #print resp
                #print content
            else:
                print "empty"

if __name__ == "__main__":
    
    key  =  '/home/lahoucine/keys/ss_key_c.pem'
    cert =  '/home/lahoucine/certs/ss_cert_c.pem'
    logA = logAgent('/var/log', SERVER_ADDR, key, cert)
    logA.run()