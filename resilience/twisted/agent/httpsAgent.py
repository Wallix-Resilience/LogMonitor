#!/usr/bin/env python

import httplib2
from resilience.twisted.agent.logWatch import LogWatcher
#
from twisted.internet import ssl, reactor
from twisted.internet.protocol import ClientFactory, Protocol
from twisted.python import log
import os
import socket
import signal
from atexit import _exithandlers

SERVER_ADDR = "https://localhost:8990"

class logAgent():
    def __init__(self, directory, server, AgentKey, AgentCertificat):
        self.server = server
        self.http = httplib2.Http(disable_ssl_certificate_validation=True)
        self.http.add_certificate(AgentKey, AgentCertificat, '')
        self.lw = LogWatcher(directory,self.sendLine)
        signal.signal(signal.SIGTERM, self._exitHandler)
        self.exit = False
    
    def _exitHandler(self):
        self.exit = True
        print "Exiting... please wait..."
        
    def run(self):
        self.lw.loop()
        
    def sendLine(self,filename,lines):
        size = 0
        for line in lines:
            if not self.exit:
                if  line.strip():
                    print "line: " ,line
                    try:
                        resp, content = self.http.request(self.server,
                                                          'POST',
                                                         line,
                                                         headers={'Content-Type': 'text/plain'}
                                                         )
                        print "resp",resp

                        if not resp['status'] == '200':
                            print "request error"
                            return size
                        size += len(line)
                        
                        
                    except socket.error, msg:
                        if socket.errno.errorcode[111] == 'ECONNREFUSED':
                            print "Socket error: connection refused"
                        return size
            else:
                return size
            
        return size

if __name__ == "__main__":

    key  =  os.path.abspath('../../../ssl/keys/ss_key_d.pem')
    cert =  os.path.abspath('../../../ssl/certs/ss_cert_d.pem')
    print key
    print cert
    logA = logAgent('/tmp/log/', SERVER_ADDR, key, cert)
    logA.run()