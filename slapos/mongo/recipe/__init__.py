import shutil
import os
import signal
from binascii import b2a_uu as uuencode
import pkg_resources


from slapos.recipe.librecipe import GenericBaseRecipe
from slapos.recipe.librecipe import BaseSlapRecipe



class BaseRecipe(GenericBaseRecipe):

  def config(self):
    path_list = [] 

    mongo_arg = dict(
      ip = self.options['ip'],
      port = self.options['port'],
      datadir = self.options['mongo-data']
    )

    mongo_conf = self.createFile(self.options['mongo-conf'],
      self.substituteTemplate(self.getTemplateFilename('mongodb.in'),
                              mongo_arg)
    )
    return mongo_conf



class Mongos(BaseRecipe):

  def install(self):
    path_list = []
    mongo_conf = self.config()
    path_list.append(mongo_conf)

    mongod_servers = " ".join((self.options['servers']).split())
    
    wrapper = self.createPythonScript(self.options['path'],
                                      'slapos.recipe.librecipe.execute.execute',
      [
      os.path.join(self.options['mongo-path'],'mongos'),'--configdb', mongod_servers, '--rest','--ipv6','--config',self.options['mongo-conf']
      ]
    )

    path_list.append(wrapper)

    return path_list



class Mongo(BaseRecipe):
  
  def install(self):
    path_list = []
    mongo_conf = self.config()
    path_list.append(mongo_conf)
        
    wrapper = self.createPythonScript(self.options['path'],
                                      'slapos.recipe.librecipe.execute.execute',
      [
      os.path.join(self.options['mongo-path'],'mongod'),'--rest','--ipv6','--config',self.options['mongo-conf']
      ]
    )

    path_list.append(wrapper)

    return path_list



class MongoConfsrv(BaseRecipe):

  def install(self):
    path_list = []
    mongo_conf = self.config()
    path_list.append(mongo_conf)
    wrapper = self.createPythonScript(self.options['path'],
                                   'slapos.recipe.librecipe.execute.execute',
        [
        os.path.join(self.options['mongo-path'],'mongod'),'--configsvr','--ipv6','--config',self.options['mongo-conf']
        ]
    )

    path_list.append(wrapper)

    return path_list



