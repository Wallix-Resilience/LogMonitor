#!/usr/bin/env python
'''
Wallix

@author: Lahoucine BENLAHMR
@contact: lbenlahmr@wallix.com ben.lahoucine@gmail.com
'''
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


KEY = "b273bca26dc6b6439765b05b88d60d3158a7214f7bc0a643a2f62825866a07fe"
#SERVER_ADDR = "https://localhost:8991/%s" % KEY
#SERVER_ADDR = "https://[2001:470:1f14:169:c00d:4cff:fed4:4894]:9983/%s" % KEY
SERVER_ADDR = "https://[fd88:9fde:bd6e:f57a:ad39:fd7b:9dea:615]:9983/%s" % KEY

class logAgent():
    """
    This class represent an example of an HTTPS agent 
    """
    def __init__(self, directory, server, AgentKey, AgentCertificat):
        """
        Initialization of a log Collection agent
        
        @param directory: path to the directory containing files log
                         to send and watch
        @param server:  address of a collection server where to send logs
        @param AgentKey: 
        @param AgentCertificat: 
        """
        self.server = server
        self.http = httplib2.Http(disable_ssl_certificate_validation=True)
        #self.http.add_certificate(AgentKey, AgentCertificat, '')
        self.lw = LogWatcher(directory,self.sendLine)
        signal.signal(signal.SIGTERM, self._exitHandler)
        self.exit = False
    
    def _exitHandler(self):
        """
        called when exiting the logAgent
        """
        self.exit = True
        print "Exiting... please wait..."
        
    def run(self):
        """
        start watching the defined directory 
        """
        self.lw.loop()
        
    def sendLine(self,filename,lines):
        """
        This function is called to send logs line to a log collection server
        @param filename: the name of the file that contain logsLine to send
        @param lines: logs data to send to collection server 
        """
        print "sending"
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
                            print resp
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
    #https://localhost:8991
    #https://[2001:470:1f14:169:c00d:4cff:fed4:4894]:9983
    parser = argparse.ArgumentParser(description='An https log agent collector')
    parser.add_argument('-s','--server',help='address of the collect server', 
                                          default=SERVER_ADDR, required=False)
    parser.add_argument('-d','--dir',help='directory to watch', 
                                          default="/tmp/log/", required=False)
    parser.add_argument('-k','--key',help='path to the certificat key', 
                                          default="../../../ssl/keys/client.pkey", required=False)
    parser.add_argument('-c','--cert',help='path to the certificat', 
                                          default="../../../ssl/certs/client.cert", required=False)

    
    args = parser.parse_args(params)
    
    serverAddr = args.server
    dirToWatch = args.dir
    k = args.key
    c = args.cert

    key  =  os.path.abspath(k)
    cert =  os.path.abspath(c)
    print key, cert
    print key
    print cert
    logA = logAgent(dirToWatch, serverAddr, key, cert)
    logA.run()
    
    
if __name__ == "__main__":
    main()
