#!/usr/bin/env python

import httplib2
from resilience.twisted.agent.logWatch import LogWatcher
#
from twisted.internet import ssl, reactor
from twisted.internet.protocol import ClientFactory, Protocol
from twisted.python import log
import os
import sys
import socket
import signal
from atexit import _exithandlers
import argparse

SERVER_ADDR = "https://[2001:470:1f14:169:f02c:cff:fe5d:285c]:9983"

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

    
    
def main():
    
    params = sys.argv[1:]
        
    parser = argparse.ArgumentParser(description='An https log agent collector')
    parser.add_argument('-s','--server',help='address of the collect server', 
                                          default="https://localhost:8990", required=True)
    parser.add_argument('-d','--dir',help='directory to watch', 
                                          default="/tmp/log/", required=True)
    parser.add_argument('-k','--key',help='path to the certificat key', 
                                          default="../../../ssl/keys/ss_key_d.pem", required=False)
    parser.add_argument('-c','--cert',help='path to the certificat', 
                                          default="../../../ssl/certs/ss_cert_d.pem", required=False)
    
    args = parser.parse_args(params)
    
    serverAddr = args.server
    dirToWatch = args.dir
    k = args.key
    c = args.cert

    key  =  os.path.abspath(k)
    cert =  os.path.abspath(c)
    print key
    print cert
    logA = logAgent(dirToWatch, serverAddr, key, cert)
    logA.run()
    
    
if __name__ == "__main__":
    main()