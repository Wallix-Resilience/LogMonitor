import shutil
import os
import signal
from binascii import b2a_uu as uuencode
import pkg_resources


from slapos.recipe.librecipe import GenericBaseRecipe
from slapos.recipe.librecipe import BaseSlapRecipe

import pkg_resources


class Producer(GenericBaseRecipe):
  def install(self):
    path_list = []
    
    ip = self.options['ip']
    port = self.options['port']
    zookeeper_addr = self.options['zookeeper-addr']
    mongo_addr = self.options['mongo-addr']
    producer_bin = self.options['producer-bin']
  
    wrapper = self.createPythonScript(self.options['path'],
                                      'slapos.recipe.librecipe.execute.execute',
                                      [ producer_bin,
                                        '-z', zookeeper_addr, '-m', mongo_addr,
                                        '-a', ip, '-p', port ]
                                      )
    path_list.append(wrapper)
    return path_list


class Consumer(GenericBaseRecipe):

  def install(self):
    path_list = []

    zookeeper_addr = self.options['zookeeper-addr']
    mongo_addr = self.options['mongo-addr']
    solr_addr = self.options['solr-addr']
    normalizer =  pkg_resources.resource_filename(__name__, "normalizers")
    print normalizer
    consumer_bin = self.options['consumer-bin']
  
    wrapper = self.createPythonScript(self.options['path'],
                                      'slapos.recipe.librecipe.execute.execute',
                                      [consumer_bin,
                                       '-z', zookeeper_addr, '-m',mongo_addr ,
                                       '-s', solr_addr, '-n', normalizer]
                                      )
    path_list.append(wrapper)
    return path_list
