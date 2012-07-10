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


class Recipe(GenericBaseRecipe):
    

  def install(self):
    path_list = []

    # Copy application
    cores_dest = os.path.join(self.options['cores-path'],"cores")
    
    try:
        shutil.rmtree(cores_dest)
    except:
        pass
    
  
    #shutil.rmtree(cores)
    
    cores_dir = pkg_resources.resource_filename(__name__,'cores')
    
    shutil.copytree(cores_dir,cores_dest)
 
   # os.mkdir(self.options['cores-path'])

    coresPath = '-Dsolr.solr.home=%s' % cores_dest
    confdir = '-Dbootstrap_confdir=%s' % cores_dest
    print confdir
    confname = '-Dcollection.configName=myconf'
    dz = '-DzkRun'
    numShard = '-DnumShards=2'
    jettyPath =  '-Djetty.home=%s' % self.options['solr-path'] 
    jettyPort = '-Djetty.port=%s' % self.options['port']
    jettyHost = '-Djetty.host=%s' % self.options['ip']
    java = os.path.join(self.options['java_home'],'bin','java')

    print coresPath
   # print self.options['java_home']
    wrapper = self.createPythonScript(self.options['path'],
        'slapos.recipe.librecipe.execute.execute',[java,
        jettyPath, confdir, coresPath, confname, jettyHost, jettyPort,   
        '-jar',os.path.join(self.options['solr-path'],'start.jar') 
        ]
    )
    
    print wrapper
    solr_config = dict(
        cores = coresPath,
        solr = self.options['solr-path']
    )
    
   
    
    path_list.append(wrapper)


    return path_list
