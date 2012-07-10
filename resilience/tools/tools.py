from mysolr import Solr
import bson
from pymongo import Connection
import gridfs

class tools():
    
    def rmFileAndIndex(self, query, solr,mongodb):
        res = solr.search(q=query)
        docs = res.documents
        mongofs = gridfs.GridFS(mongodb)

        while docs:
            for doc in docs:
                fileid = doc["fileid"]
                uuid = "uuid:%s" % doc["uuid"]          
                solr.delete_by_query(uuid)
                
                #print mongodb.fs.files.find_one({'_id':bson.ObjectId(fileid)})
                mongodb.fs.files.update({'_id':bson.ObjectId(fileid)},{"$inc":{"remLines":-1}})
                fileinfo =  mongodb.fs.files.find_one({'_id':bson.ObjectId(fileid)})
                print fileinfo
                print doc
                if not fileinfo == None and fileinfo["remLines"] == 0:
                    print fileinfo["filename"], "will be deleted"
                    mongofs.delete(bson.ObjectId(fileid))
                          

            res = solr.search(q=query)
            docs = res.documents
        
    def purgeIndex(self,since):
        pass  
    
    def purgeStorage(self,since):
        pass
    
    def purgeIndexStorage(self,since):
        pass
    
    def getfile(self, id, dest):
        pass
    
          
    
    
if __name__ == "__main__":
    
    solr = Solr('http://localhost:8983/solr/collection1/')
    connection = Connection()
    db = connection["resilience3"]
    t = tools()
    t.rmFileAndIndex("*:*", solr, db)