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



class Recipe(GenericBaseRecipe):
    

  def install(self):
    path_list = [] 

    
    # Install lighty
    zoo_arg = dict(
      ip = self.options['ip'],
      port = self.options['port'],
      datadir = self.options['zookeeper-data']
    )

    zookeeper_conf = self.createFile(self.options['zookeeper-conf'],
      self.substituteTemplate(self.getTemplateFilename('zookeeper.in'),
                              zoo_arg)
    )

    path_list.append(zookeeper_conf)
    
    java = os.path.join(self.options['java_home'],'bin','java')
    logdir = '-Dzookeeper.log.dir="%s"' % self.options['zookeeper-log']
    looger = '-Dzookeeper.root.logger="INFO,CONSOLE"'
    jmx = '-Dcom.sun.management.jmxremote' 
    jmxl = '-Dcom.sun.management.jmxremote.local.only=false'
    zoobin = self.options['zookeeper-path']
    zooconf = self.options['zookeeper-conf']
    paths = [ os.path.join(zoobin,'../build/classes:'),
           os.path.join(zoobin,'../build/lib/*.jar:'),
           os.path.join(zoobin,'../lib/slf4j-log4j12-1.6.1.jar:'),
           os.path.join(zoobin,'../lib/slf4j-api-1.6.1.jar:'),
           os.path.join(zoobin,'../lib/netty-3.2.2.Final.jar:'),
           os.path.join(zoobin,'../lib/log4j-1.2.15.jar:'),
           os.path.join(zoobin,'../lib/jline-0.9.94.jar:'),
           os.path.join(zoobin,'../zookeeper-3.4.3.jar:'),
           os.path.join(zoobin,'../src/java/lib/*.jar:'),
           os.path.join(zoobin,'../conf:') ]

    cp = "".join(paths)


    wrapper = self.createPythonScript(self.options['path'],
        'slapos.recipe.librecipe.execute.execute',
        [
        java, '-cp', cp, jmx, jmxl,
        'org.apache.zookeeper.server.quorum.QuorumPeerMain', zooconf,
        ]
    )
      

    path_list.append(wrapper)

    return path_list
