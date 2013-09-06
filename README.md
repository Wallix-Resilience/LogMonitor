LOG COLLECTOR FOR CLOUD
============================

Infos:
------
Author: Lahoucine BENLAHMR
Contact: lbe@wallix.com
Company: Wallix


Description:
------------
this software is a distributed and fault tolerant log collector for Cloud

requirements:
------------

- Apache SolrCloud
- Apache Zookeeper
- Mongodb

How to:
-------
1. Start zookeeper
2. Start and register mongodb and Solr in Zookeeper using the bootstrap script mongo.py and solr.py
4. To start an instance of producer use producerNg.py
5. To start an instance of consumer use consumerNg.py
6. use the CLI script to communicat with the log collector an do the following operations:
   - add a new source of log collect
   - remove a source of log collect
   - make a search query in the indexed logs
   - remove logs
   - get a log file
   - remove a log file
   - get a list of declared sources
   - change the admin password
   - purge index and storage
7. to send logs after the declarion of the collection source. use httpsAgent to watch a directory and send logs 

