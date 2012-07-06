#!/usr/bin/env python


import os
import time
import errno
import stat
import sqlite3

class LogWatcher(object):


    def __init__(self, folder, callback, extensions=["log",]):
        self.files_map = {}
        self.callback = callback
        self.folder = os.path.realpath(folder)
        self.extensions = extensions
        assert os.path.isdir(self.folder), "%s does not exists" \
                                            % self.folder
        assert callable(callback)
        self.connection = sqlite3.connect("database.db")
        try:
            self.connection.cursor().execute('CREATE TABLE files (fid TEXT PRIMARY KEY, position INTEGER)')
        except Exception as err:
            print('issue while creating database: %s' % err)
        self.update_files()

        

    def __del__(self):
        self.close()

    def loop(self, interval=0.1, async=False):
        """Start the loop.
        If async is True make one loop then return.
        """
        while 1:
            self.update_files()
            for fid, file in list(self.files_map.iteritems()):
                self.readfile(file)
            if async:
                return
            time.sleep(interval)

    def log(self, line):
        """Log when a file is un/watched"""
        print line

    def listdir(self):
        """List directory and filter files by extension."""
        ls = os.listdir(self.folder)
        if self.extensions:
            return [x for x in ls if os.path.splitext(x)[1][1:] \
                                           in self.extensions]
        else:
            return ls

    
    def update_files(self):
        ls = []
        for name in self.listdir():
            absname = os.path.realpath(os.path.join(self.folder, name))
            try:
                st = os.stat(absname)
            except EnvironmentError, err:
                if err.errno != errno.ENOENT:
                    raise
            else:
                if not stat.S_ISREG(st.st_mode):
                    continue
                fid = self.get_file_id(st)
                ls.append((fid, absname))

        # check existent files
        for fid, file in list(self.files_map.iteritems()):
            try:
                st = os.stat(file.name)
            except EnvironmentError, err:
                if err.errno == errno.ENOENT:
                    self.unwatch(file, fid)
                else:
                    raise
            else:
                if fid != self.get_file_id(st):
                    # same name but different file (rotation); reload it.
                    self.unwatch(file, fid)
                    self.watch(file.name)

        # add new ones
        for fid, fname in ls:
            if fid not in self.files_map:
                self.watch(fname)

    def readfile(self, file):
        pos = 0 
        fid = None
        try:
            fid = self.get_file_id(os.stat(file.name))
            res = self.connection.cursor().execute("select position from files where fid =?", (fid,) )
            row = res.fetchone()
            pos = row[0]
        except Exception as err:
            print("Issue while retriving position of %s" % file)
       # print "position file:", file.name, pos
        file.seek(pos)
        lines = file.readlines()
        
        size = os.path.getsize(file.name)
        try:
            self.connection.cursor().execute("UPDATE files SET position=? where fid=?",(size,fid))
            self.connection.commit()
         #   print file.name," database updated"
        except Exception as err:
            print("issue while updating position of the file %s: %s",(file.name,err))
            
        if lines:
            self.callback(file.name, lines)

    def watch(self, fname):
        try:
            file = open(fname, "r")
            fid = self.get_file_id(os.stat(fname))
        except EnvironmentError, err:
            if err.errno != errno.ENOENT:
                raise
        else:
            self.log("watching logfile %s" % fname)
            self.files_map[fid] = file
            try:
                self.connection.cursor().execute('INSERT INTO files VALUES(?,?)',
                                               (fid,0))
                self.connection.commit()
            except Exception as err:
                print('issue while inserting %s in the database: %s' % (fname,err))

    def unwatch(self, file, fid):
        # file no longer exists; if it has been renamed
        # try to read it for the last time in case the
        # log rotator has written something in it.
        lines = self.readfile(file)
        self.log("un-watching logfile %s" % file.name)
        del self.files_map[fid]
        if lines:
            self.callback(file.name, lines)

    @staticmethod
    def get_file_id(st):
        return "%xg%x" % (st.st_dev, st.st_ino)

    def close(self):
        for id, file in self.files_map.iteritems():
            file.close()
        self.files_map.clear()

    #
    def add_file(self):
        pass
    
    def add_folder(self):
        pass
    
    def remove_file(self):
        pass
    
    def remove_folder(self):
        pass
    
    
    
        
if __name__ == '__main__':
    def callback(filename, lines):
        #print lines
        print " reading file: ", filename
        for line in lines:
            line.rstrip("\n")
            print filename, " => ",line

    l = LogWatcher("/var/log/", callback)
    l.loop()
