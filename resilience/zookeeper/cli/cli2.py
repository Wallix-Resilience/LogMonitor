#!/usr/bin/env python
'''
Created on 9 janv. 2013

@author: lahoucine
'''
import argparse
import sys
import httplib2
from urllib import urlencode
import socket



def request(server, data):
    try:
        http = httplib2.Http(disable_ssl_certificate_validation=True)
        resp, content = http.request(server,
                                     'POST',
                                     headers={'Content-type': 'application/x-www-form-urlencoded'},
                                     body=urlencode(data),
                                     )
        
        return (resp["status"], content)
    except socket.error:
        if socket.errno.errorcode[111] == 'ECONNREFUSED':
            print "Socket error: connection refused"
        return (None, None)
    except Exception, e:
        print "Error: ", e
        return (None, None)
    

def addSource(args): 
    data = { 'user': args.user, 'password': args.password, 'source': args.sourceName}
    server = "https://%s/%s" % (args.server, "register")
    respcode , content = request(server, data)
    if respcode == "200":
        print "This is your key, keep it secret!: ", content
    else:
        print "erro"
    
def removeSource(args):
    data = { 'user': args.user, 'password': args.password, 'source': args.sourceName}
    server = "https://%s/%s" % (args.server, "remove")
    respcode , content = request(server, data)
    if respcode == "200":
        print "source %s was removed" % args.sourceName
    else:
        print "erro"
        
        
def search(args):
    data = { 'user': args.user, 'password': args.password, 'query': args.query}
    server = 'https://%s/%s' % (args.server, 'search')
    respcode , content = request(server, data)
    if respcode == '200':
        print content
    else:
        print "erro"
        
        
    
def main():
    params = sys.argv[1:]
    parser = argparse.ArgumentParser(description='Resilient Log configuration client')
    
    parser.add_argument('-s','--server',help='address of the producer server', 
                                          default="localhost:8991")
    parser.add_argument('-u', '--user', help='Administrator user name', default='admin')
    parser.add_argument('-p', '--password', help='Administrator password')
    subparsers = parser.add_subparsers(help='sub-command help')
    
    #create the parser for the 'addSource' command
    parserAddSource = subparsers.add_parser('addSource', help='add Source')
    parserAddSource.add_argument('sourceName')
    parserAddSource.set_defaults(func=addSource)
    
    #create the parser for the 'removeSource' command
    parserRmSource = subparsers.add_parser('removeSource', help='remove Source')
    parserRmSource.add_argument('sourceName')
    parserRmSource.set_defaults(func=removeSource)
    
    #create the parser for the 'search' command
    parserSearch = subparsers.add_parser('search', help='search')
    parserSearch.add_argument('query')
    parserSearch.set_defaults(func=search)
    
    
    #create the parser for the 'getFile' command
    parserAddSource = subparsers.add_parser('getFile', help='get a file')
    parserAddSource.add_argument('query', help='query')
    
    #create the parser for the 'removeFile' command
    parserAddSource = subparsers.add_parser('removeFile', help='remove a file')
    parserAddSource.add_argument('query')
    
    #create the parser for the 'removeLog' command
    parserAddSource = subparsers.add_parser('removeLog', help='remove a log Line')
    parserAddSource.add_argument('query')
    
    #create the parser for the 'listSource' command
    parserAddSource = subparsers.add_parser('listSource', help='list Sources')
 
    #create the parser for the 'changePass' command
    parserAddSource = subparsers.add_parser('changePass', help='change the admin Password')
    parserAddSource.add_argument('password')
    
    
    args = parser.parse_args(params)
    args.func(args)

if __name__ == '__main__':
    main()
        
         
   
    

    