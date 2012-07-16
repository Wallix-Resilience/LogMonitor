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
                  'resilience.twisted',
                  'resilience.twisted.agent',
                  'resilience.twisted.server'],
      requires=['pylogsparser','solrpy','mysolr','pymongo'],
      entry_points = {        
        'console_scripts': [
            'producer = resilience.zookeeper.producersConsumers.producerNg:main',
            'consumer = resilience.zookeeper.producersConsumers.consumerNg:main',
            'collectAgent = resilience.twisted.agent.httpsAgent:main',
        ],
      }
)
