#!/usr/bin/env python

import argparse
import sys
import httplib2
from urllib import urlencode
import socket
from twisted.web import http
import getpass
import simplejson as json

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

def getUser(args):
    if not args.user:
        return raw_input('Please provide the admin account (default: admin) : ') or "admin"
    return args.user

def getPassword(args):
    if not args.password:
        return getpass.getpass()
    else:
        return args.password

def addSource(args): 
    data = { 'user': getUser(args), 'password': getPassword(args), 'source': args.sourceName, 'ip' : args.sourceIP}
    server = "https://%s/%s" % (args.server, "register")
    respcode , content = request(server, data)
    respcode = int(respcode)
    if respcode == http.OK:
        print "This is your key, keep it secret!: ", content
    elif respcode == http.UNAUTHORIZED:
        print "Bad user or/and password"
    elif respcode == http.CONFLICT:
        print "The resource %s already exist!" % args.sourceName
    
def removeSource(args):
    data = { 'user': getUser(args), 'password': getPassword(args), 'source': args.sourceName}
    server = "https://%s/%s" % (args.server, "remove")
    respcode , content = request(server, data)
    respcode = int(respcode)
    if respcode == http.OK:
        print "source %s was removed" % args.sourceName
    elif respcode == http.BAD_REQUEST:
        print "Erro will removing %s" % args.sourceName
    elif respcode == http.UNAUTHORIZED:
        print "Bad user or/and password"    
        
def search(args):
    rows = 500
    if args.rows:
        rows = int(args.rows)
    data = { 'user': getUser(args), 'password': getPassword(args), 'query': args.query, 'rows': rows}
    server = 'https://%s/%s' % (args.server, 'search')
    respcode , content = request(server, data)
    respcode = int(respcode)
    if respcode == http.OK:
        content_data = json.loads(content)
        #print simplejson.dumps(simplejson.loads(content), ident=4)
        #print content_data
        #pprint.pprint(content)
        print content  
    else:
        print "Error"
        
        
def changePass(args):
    
    new_user_name = args.user
    new_password = args.password
    #do you want to change the adminuser name:
    change_user = raw_input('Do you want to change the administrator user name? [NO, YES] : ')
    change_user = change_user.upper()
    if change_user not in ("YES", "NO"):
        print "please choose YES or NO"
        return 
    if change_user == "YES":
        new_user_name = raw_input("Please enter the new admin username: ")
        
    change_password = raw_input('Do you want to change the administrator password? [NO, YES] : ')
    change_password = change_password.upper()
    if change_password not in ("YES", "NO"):
        print "Please choose YES or NO"
        return
    
    if change_password == "YES":
        new_password = getpass.getpass("Please enter the new administrator password: ")
        new_password_bis = getpass.getpass("Please enter AGAIN the new administrator password: ")
        if not new_password == new_password_bis:
            print "the entred passwords dont match!"
            return
        
    if change_password == "NO" and change_user == "NO":
        print "Operation aborted"
        return
    
    user = getUser(args)
    password = getPassword(args)
    data = { 'oUser': user , 'oPass': password, 'nUser': new_user_name or user, 'nPass': new_password or password}
    print data
    server = 'https://%s/%s' % (args.server, 'change')
    print server
    respcode , content = request(server, data)
    respcode = int(respcode)
    print respcode
    if respcode == http.OK:
        print "Operation was succed"
    else:
        print "Error operation"
        
def getFile(args):
    data = { 'user': getUser(args), 'password': getPassword(args), 'fileID': args.fileID}
    server = 'https://%s/%s' % (args.server, 'getFile')
    respcode , content = request(server, data)
    respcode = int(respcode)
    if respcode == http.OK:
        print content


def getSources(args):
    data = { 'user': getUser(args), 'password': getPassword(args)}
    server = 'https://%s/%s' % (args.server, 'getSources')
    respcode , content = request(server, data)
    respcode = int(respcode)
    if respcode == http.OK:
        print content


def purgeStorage(args):
    data = { 'user': getUser(args), 'password': getPassword(args)}
    server = 'https://%s/%s' % (args.server, 'purgeStorage')
    respcode , content = request(server, data)
    respcode = int(respcode)
    if respcode == http.OK:
        print content
        
def removeFile(args):
    data = { 'user': getUser(args), 'password': getPassword(args), 'fileid': args.fileid}
    server = 'https://%s/%s' % (args.server, 'deleteFile')
    if args.propagate:
        data['propagate'] = 'True'
    respcode , content = request(server, data)
    respcode = int(respcode)
    if respcode == http.OK:
        print "File were removed"
    elif respcode == http.BAD_REQUEST:
        print "Erro will removing %Logs"
    elif respcode == http.UNAUTHORIZED:
        print "Bad user or/and password"  


def removeLogs(args):
    data = { 'user': getUser(args), 'password': getPassword(args), 'query': args.query}
    server = 'https://%s/%s' % (args.server, 'deleteLogs')
    if args.propagate:
        data['propagate'] = 'True'
    respcode , content = request(server, data)
    respcode = int(respcode)
    if respcode == http.OK:
        print "Logs were removed"
    elif respcode == http.BAD_REQUEST:
        print "Erro will removing %Logs"
    elif respcode == http.UNAUTHORIZED:
        print "Bad user or/and password"  


def main():
    params = sys.argv[1:]
    parser = argparse.ArgumentParser(description='LogMonitor configuration client')
    
    parser.add_argument('-s','--server',help='address of the producer/collector server', 
                                          default="localhost:8991")
    parser.add_argument('-u', '--user', help='Administrator user name')
    parser.add_argument('-p', '--password', help='Administrator password')
    subparsers = parser.add_subparsers(help='sub-command help')
    
    #create the parser for the 'addSource' command
    parserAddSource = subparsers.add_parser('addSource', help='add a source of log collection to the authorisation list')
    parserAddSource.add_argument('sourceName')
    parserAddSource.add_argument('sourceIP')
    parserAddSource.set_defaults(func=addSource)
    
    #create the parser for the 'removeSource' command
    parserRmSource = subparsers.add_parser('deleteSource', help='remove a source of log collection from the authorisation list')
    parserRmSource.add_argument('sourceName')
    parserRmSource.set_defaults(func=removeSource)
    
    #create the parser for the 'search' command
    parserSearch = subparsers.add_parser('search', help='make a search query')
    parserSearch.add_argument('query')
    parserSearch.add_argument('-r', '--rows', required=False)
    parserSearch.set_defaults(func=search)
    
    #create the parser for the 'getFile' command
    parserGetFile = subparsers.add_parser('getFile', help='get a log file from the storage')
    parserGetFile.add_argument('fileID', help='get a log file')
    parserGetFile.set_defaults(func=getFile)
    
    #create the parser for the 'deleteFile' command
    parserRemoveFile = subparsers.add_parser('deleteFile', help='remove a file from the file storage')
    parserRemoveFile.add_argument('fileid')
    parserRemoveFile.add_argument('-f', '--propagate', action="store_true", required=False, 
                                                         help='remove also file\'s logs lines from the index storage')
    parserRemoveFile.set_defaults(func=removeFile)
    
    #create the parser for the 'deleteLogs' command
    parserRemoveLog = subparsers.add_parser('deleteLogs', help='remove log Lines from the index storage')
    parserRemoveLog.add_argument('query')
    parserRemoveLog.add_argument('-f', '--propagate', action="store_true", required=False, 
                                                         help='remove also the file referenced by the log lines if not referenced anymore')
    parserRemoveLog.set_defaults(func=removeLogs)
    
    #create the parser for the 'purgeStorafge' command
    parserPurge = subparsers.add_parser('purgeStorage', help='remove Files not referenced in the index storage')
    parserPurge.set_defaults(func=removeLogs)

    #create the parser for the 'getSources' command
    parserListSources = subparsers.add_parser('getSources', help='list Sources in the authorisation list')
    parserListSources.set_defaults(func=getSources)
    
    #create the parser for the 'changePass' command
    parserAddSource = subparsers.add_parser('changePass', help='change the admin Password')
    parserAddSource.set_defaults(func=changePass)

    
    
    args = parser.parse_args(params)
    args.func(args)

if __name__ == '__main__':
    main()   