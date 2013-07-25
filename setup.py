from setuptools import setup, find_packages

setup(name='ResilientLog',
      version='1.0',
      description='A light logbox for the cloud',
      author='Lahoucine benlahmr',
      author_email='lbenlahmr@wallix.com',
      url='http://www.wallix.com',
      packages = ['resilience',
                  'resilience.zookeeper',
                  'resilience.zookeeper.producersConsumers',
                  'resilience.zookeeper.configure',
                  'resilience.twisted',
                  'resilience.twisted.agent',
                  'resilience.twisted.server',
                  'resilience.cli',
                  'resilience.bootstrap',
                  'resilience.data'],      
      package_data={'resilience': ['conffiles/*']},
      requires=['pylogsparser','solrpy','mysolr','pymongo'],
      entry_points = {        
        'console_scripts': [
            'producer = resilience.zookeeper.producersConsumers.producerNg:main',
            'consumer = resilience.zookeeper.producersConsumers.consumerNg:main',
            'collectAgent = resilience.twisted.agent.httpsAgent:main',
            'solr = resilience.bootstrap.solr:main',
            'mongo = resilience.bootstrap.mongo:main',
            'mongoshardinit = resilience.bootstrap.mongo:initsharding',
            'mongoaddshards = resilience.bootstrap.mongo:addshards',
            'cli = resilience.cli.cli:main'
        ],
      }
)
