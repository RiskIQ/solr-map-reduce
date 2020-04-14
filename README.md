# Solr MapReduce
[![Build Status](https://secure.travis-ci.org/RiskIQ/solr-map-reduce.png?branch=master)](http://travis-ci.org/RiskIQ/solr-map-reduce)

## What?
This library provides utilities for creating Solr indexes using mapreduce.

## Why?
The code contained in this repository was initially taken from [version 6.5.1](https://github.com/apache/lucene-solr/tree/releases/lucene-solr/6.5.1) 
of the [apache lucene-solr](https://github.com/apache/lucene-solr) codebase.  Starting with version 6.6.0, these modules 
were dropped from the codebase, as its maintainers were no longer interested in supporting them. 

As heavy users of this library, we feel that these tools are still useful and valuable, and that they should continue to 
be maintained and made available to the open-source community.  

## How?
Maven:
```xml
<dependency>
    <groupId>com.riskiq</groupId>
    <artifactId>solr-map-reduce</artifactId>
    <version>8.4.1.2</version>
</dependency>
```

Gradle:
```groovy
compile group: 'com.riskiq', name: 'solr-map-reduce', version: '8.4.1.2'
```

Example lifted from [the original readme](https://github.com/apache/lucene-solr/tree/releases/lucene-solr/6.5.1/solr/contrib/map-reduce):

```bash
# Build an index with map-reduce and deploy it to SolrCloud

source $solr_distrib/example/scripts/map-reduce/set-map-reduce-classpath.sh

$hadoop_distrib/bin/hadoop --config $hadoop_conf_dir jar \
$solr_distrib/dist/solr-map-reduce-*.jar -D 'mapred.child.java.opts=-Xmx500m' \
-libjars "$HADOOP_LIBJAR" --morphline-file readAvroContainer.conf \
--zk-host 127.0.0.1:9983 --output-dir hdfs://127.0.0.1:8020/outdir \
--collection $collection --log4j log4j.properties --go-live \
--verbose "hdfs://127.0.0.1:8020/indir"
```