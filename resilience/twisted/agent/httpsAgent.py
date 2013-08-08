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
import getpass
from urlparse import urljoin

class logAgent():
    """
    This class represent an example of an HTTPS agent 
    """
    def __init__(self, directory, server, key):
        """
        Initialization of a log Collection agent
        
        @param directory: path to the directory containing files log
                         to send and watch
        @param server:  address of a collection server where to send logs
        @param AgentKey: 
        @param AgentCertificat: 
        """
        self.server = urljoin('https://%s' % server, 'send/%s' % key)
        self.http = httplib2.Http(disable_ssl_certificate_validation=True)
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
    parser = argparse.ArgumentParser(description='An https log agent collector')
    parser.add_argument('-s','--server',help='address of the collect server', required=True)
    parser.add_argument('-d','--dir',help='directory to watch', required=True)
    args = parser.parse_args(params) 
    serverAddr = args.server
    dirToWatch = args.dir
    key = getpass.getpass("Enter your Key: ")
    print key
    logA = logAgent(dirToWatch, serverAddr, key)
    logA.run()
    
    
if __name__ == "__main__":
    main()
