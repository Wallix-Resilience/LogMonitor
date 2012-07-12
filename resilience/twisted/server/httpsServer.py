#!/usr/bin/python
import sys
from pprint import pprint
from twisted.web.server import Site
from twisted.web.resource import Resource
from twisted.internet import reactor
from twisted.python import log

import sqlite3
log.startLogging(sys.stdout)


class LogCollect(Resource):
    isLeaf = True
    def __init__(self,producer):
        self.prod = producer   
        self.connection = sqlite3.connect("certificat.db")
        try:
            #self.connection.cursor().execute('CREATE TABLE nodes (name TEXT, certificat TEXT, PRIMARY KEY(name, certificat))')
            self.connection.cursor().execute('INSERT INTO nodes VALUES(?,?)',("machine2","87:88:16:93:9D:95:C1:83:B1:B7:1B:F6:14:FE:2A:67"))
            self.connection.commit()
        except Exception as err:
            print('issue while creating database: %s' % err)
     
    def render_POST(self, request):
        l = dir(request)
        print l
        print "IP:", request.getClientIP()
        print "Is secure:", request.isSecure()
        print "Client:", request.getClient()
     
        
        tr = request.transport
        crt = tr.getPeerCertificate()
        md5 = crt.digest("md5")
        print md5
        name = None 
        try:
            res = self.connection.cursor().execute('SELECT name FROM nodes where certificat=?', (md5,) )
            row = res.fetchone()               
            name = row[0]
            print "machine name:", name
        except Exception as err:
            print ('issue while select: %s' % err)
                    
        logProcess(request, self.prod, name)
        #print newdata
        return ''
    
def initServerFactory(producer):  
    root = LogCollect(producer)
    factory = Site(root)
    return factory    



def logProcess(request, producer, name):
    line = request.content.getvalue()
    if  line.strip():
        print "line: ",line
        producer.produce(line, name)
    else:
        print "empty"
