'''
Created on 3 juil. 2013

@author: lahoucine
'''

from zope.interface import Interface, implements
from twisted.python import log
from datetime import datetime, timedelta, tzinfo
import solr #solrpy
from dateutil import tz
from mysolr import Solr
from pymongo import Connection


# Solr hates unicode control chars...
UNICODE_CONTROL_CHARACTERS = {}
for ordinal in range(0x20):
    if chr(ordinal) not in '\t\r\n':
        UNICODE_CONTROL_CHARACTERS[ordinal] = None
        
class IDataIndexer(Interface):
    
    def index(self,data):
        pass
    
    def commit(self):
        pass
    
    def search(self, query):
        pass


class SolrIndexer(object):
    implements(IDataIndexer)
    
    
    def __init__(self, conf):
        self.solr = None
        self.conf = conf
        self._init_solr()
    
    def index(self,data):
        """
        index data in solr
        @param data: data to index into solr
        @return: true if indexing succeed 
        """
        self.update_wlog_for_solr(data) 
        for key, value in data.items():
            if isinstance(value,datetime):
                data[key] = self._utc_to_string(value)
        try:
            print "json:", data
            self.solr.update([data],commit=False)
            return True
        except Exception, e:
            log.msg("WARNING unable to index %s due to : %s" % (data,e))
            return False
        
    # Yoinked from wlb
    def update_wlog_for_solr(self, wlog):
        # Remove some control characters (Solr hates it)
        for key in wlog.keys():
            if isinstance(wlog[key], unicode):
                wlog[key] = wlog[key].translate(UNICODE_CONTROL_CHARACTERS)
        
        received_at = datetime.utcnow()
        if not 'date' in wlog:
            date = received_at
        else:
            date = wlog['date']
        if not 'body' in wlog:
            wlog['body'] = wlog['raw']
            
        wlog['__d_seconds'] = date.second
        wlog['__d_ms'] = date.microsecond
        wlog['__r_seconds'] = received_at.second
        wlog['__r_ms'] = received_at.microsecond
        wlog['date'] = date.replace(tzinfo=UTC, second=0, microsecond=0)
        received_at = received_at.replace(tzinfo=UTC, second=0, microsecond=0)
        wlog['received_at'] = self._utc_to_string(received_at)
        
    def _utc_to_string(self,data):
        """
        convert utc to string
        @param data: utc data
        @return: the string value of utc
        """
        try:
            value = solr.core.utc_to_string(data)
        except:
            pst = tz.gettz('Europe/Paris')
            value = value.replace(tzinfo=pst)
            value = solr.core.utc_to_string(data)
        return value
    
    
    def search(self, query, rows=500):
        q = { 'q' : query, 'rows':rows}
        try:
            resp = self.solr.search(**q)
            return resp.documents
        except Exception, e:
            print e
            return []
        
    def delete_by_query(self, query):
        try:
            self.solr.delete_by_query(query)
        except:
            pass
    
    def commit(self):
        """
        Make a commit in Solr and manage the commit timer
        @param stop: if true, the timer will be stopped else it will be reset
        """
        try:
            self.solr.commit()
            log.msg("commit in solr")
        except:
            log.msg("can't commit in solr")
        
    def _init_solr(self):
        """Initialization of a Solr instance 
        """
        def _call(s):
            """Retrieve Solr configuration from Zookeeper
            and initialize a solr client             
            """
            if s:
                addr, sep, port =s[0].rpartition(":")
                #if addr.count(':') > 0:                
                #    solrAddr = "http://[%s]:%s/solr/collection1/" % (addr, port)
                #    print "addddr", solrAddr
                #else:
                solrAddr = "http://%s:%s/solr/collection1/" % (addr, port)
                print "addddr", solrAddr
                try:
                    self.solr = Solr(solrAddr)
                    log.msg("connected to solr: %s" % solrAddr )
                except Exception, e:
                    log.msg("can't connect to solr: %s" % e)
                
        self.conf.get_solr_all(_call)

# Yoinked from python docs
ZERO = timedelta(0)
class Utc(tzinfo):
    """UTC tzinfo instance
    """
    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO
UTC = Utc()