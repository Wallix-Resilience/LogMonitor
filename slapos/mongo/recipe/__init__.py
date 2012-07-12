##############################################################################
#
# Copyright (c) 2010 Vifib SARL and Contributors. All Rights Reserved.
#
# WARNING: This program as such is intended to be used by professional
# programmers who take the whole responsibility of assessing all potential
# consequences resulting from its eventual inadequacies and bugs
# End users who are looking for a ready-to-use solution with commercial
# guarantees and support are strongly adviced to contract a Free Software
# Service Company
#
# This program is Free Software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
#
##############################################################################
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



