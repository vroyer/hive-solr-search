Introduction
============

Apache SOLR is a power full search engine. With this Hive SOLR Search Storage Handler, you can combine SOLR search with Hive QL to achieve powerfull analytics. 

If you are you using Datastax Entreprise, Cassandra tables can be indexed by SOLR (see http://www.datastax.com/documentation/datastax_enterprise/4.5/datastax_enterprise/srch/srchIntro.html) and a SOLR search should be much faster than a MapReduce job.  


Installation
============

To install:

	$ git clone http://github.org/vroyer/hive-solr-search
	$ cd hive-solr-search
	$ mvn package
	$ cp target/hive-solr-search-0.1-SNAPSHOT.jar `$HIVE_HOME/lib`
	$ cp target/hive-solr-search-0.1-SNAPSHOT.jar `$HADOOP_HOME/lib`

Basic Usage
===========

The following examples rely on a slightly modified version of the Datastax SOLR demo using Wikipedia as an example of Solr capabilities (see http://www.datastax.com/documentation/datastax_enterprise/4.5/datastax_enterprise/srch/srchDemoSolr.html). The date field has been modified from type text to timestamp.

Create an external hive table, map SOLR document fields to columns and provide a search query string.

	CREATE EXTERNAL TABLE hive_solr_wiki ( id INT, date TIMESTAMP, title  STRING, name STRING) 
	STORED BY "org.vroyer.hive.solr.SolrStorageHandler" 
	WITH SERDEPROPERTIES ( "solr.document.mapping" = "id,date,title,name")
	TBLPROPERTIES ("solr.url" = "http://localhost:8983/solr/wiki.solr", "solr.qs"="q=title:%22List of solar%22" );

	hive> SELECT * FROM hive_solr_wiki;
	OK
	23755339	2010-07-17 01:50:13.998	List of solar eclipses in the 17th century	23755339
	23757234	2009-07-29 21:13:44.998	List of solar eclipses in the 6th century	23757234
	23757377	2009-07-29 21:14:34.998	List of solar eclipses in the 4th century	23757377
	23758716	2010-07-14 18:04:19.998	List of solar eclipses in the 24th century	23758716
	23755556	2011-01-17 14:04:13.998	List of solar eclipses in the 15th century	23755556
	23756230	2010-10-15 18:23:14.998	List of solar eclipses in the 10th century	23756230
	23757484	2010-07-14 17:36:05.998	List of solar eclipses in the 2nd century	23757484
	23758625	2010-07-14 18:02:17.998	List of solar eclipses in the 23rd century	23758625
	23755630	2010-05-29 21:11:59.998	List of solar eclipses in the 14th century	23755630
	23755825	2010-05-29 21:49:00.998	List of solar eclipses in the 12th century	23755825
	23757324	2010-04-22 03:15:34.998	List of solar eclipses in the 5th century	23757324
	23758509	2011-02-25 14:20:48.998	List of solar eclipses in the 22nd century	23758509
	23755450	2010-05-29 21:01:17.998	List of solar eclipses in the 16th century	23755450
	23755698	2010-05-29 21:44:19.998	List of solar eclipses in the 13th century	23755698
	23755246	2010-07-14 20:25:13.998	List of solar eclipses in the 18th century	23755246
	23757086	2009-07-29 21:08:58.998	List of solar eclipses in the 7th century	23757086
	23757446	2009-07-29 21:16:17.998	List of solar eclipses in the 3rd century	23757446
	23757589	2010-10-13 23:31:07.998	List of solar eclipses in the 1st century	23757589
	23756102	2009-07-28 23:12:51.998	List of solar eclipses in the 11th century	23756102
	23756391	2009-07-29 21:03:26.998	List of solar eclipses in the 8th century	23756391
	23754780	2010-07-14 17:59:02.998	List of solar eclipses in the 19th century	23754780
	23756290	2009-07-29 21:01:51.998	List of solar eclipses in the 9th century	23756290
	Time taken: 0.847 seconds, Fetched: 22 row(s)

SOLR Faceting
=============

You can also use a SOLR facets to aggregate some data like a GROUP BY, here in a range facet on a date field.

	CREATE EXTERNAL TABLE hive_solr_facet_search ( date TIMESTAMP, count BIGINT)
	STORED BY "org.vroyer.hive.solr.SolrStorageHandler"
	WITH SERDEPROPERTIES ( "solr.facet.mapping" = "ranges")
	TBLPROPERTIES (
	   "solr.url" = "http://localhost:8983/solr/wiki.solr", 
	   "solr.qs"="q=*%3A*&facet=true&facet.range=date&facet.range.gap=%2B1YEAR&f.date.facet.range.start=NOW/YEAR%2D10YEARS&f.date.facet.range.end=NOW"
	);
	
	
	hive> SELECT * FROM hive_solr_facet_search;
	OK
	2004-01-01 00:59:59.998	0
	2005-01-01 00:59:59.998	0
	2006-01-01 00:59:59.998	0
	2007-01-01 00:59:59.998	0
	2008-01-01 00:59:59.998	0
	2009-01-01 00:59:59.998	194
	2010-01-01 00:59:59.998	551
	2011-01-01 00:59:59.998	2834
	2012-01-01 00:59:59.998	0
	2013-01-01 00:59:59.998	0
	Time taken: 0.704 seconds, Fetched: 10 row(s)
	
You could get the same result with the following Hive query.

	hive>SELECT year(date), count(*) FROM solr GROUP BY year(date);
	Total MapReduce jobs = 1
	Launching Job 1 out of 1
	Number of reduce tasks not specified. Estimated from input data size: 1
	Starting Job = job_201410251754_0010, Tracking URL = http://localhost:50030/jobdetails.jsp?jobid=job_201410251754_0010
	Kill Command = /Application/dse-4.5.2/bin/dse hadoop job  -kill job_201410251754_0010
	Hadoop job information for Stage-1: number of mappers: 2; number of reducers: 1
	2014-11-11 01:21:46,150 Stage-1 map = 0%,  reduce = 0%
	2014-11-11 01:22:36,474 Stage-1 map = 50%,  reduce = 0%
	2014-11-11 01:22:42,994 Stage-1 map = 100%,  reduce = 0%
	2014-11-11 01:23:02,152 Stage-1 map = 100%,  reduce = 100%
	Ended Job = job_201410251754_0010
	MapReduce Jobs Launched: 
	Job 0: Map: 2  Reduce: 1   HDFS Read: 0 HDFS Write: 0 SUCCESS
	Total MapReduce CPU Time Spent: 0 msec
	OK
	2009	194
	2010	551
	2011	2834
	Time taken: 121.566 seconds, Fetched: 3 row(s)

The solr.split.size property (default is 100 000) limits the number of results updated or fetched in a SOLR request. 

Pushdown filtering
==================

If you set hive.optimize.ppd=true, the SolrStorageHandler parses the WHERE clause to build a SOLR filter query expression (fq parameter). For example, the following query generates a SOLR request with fq=title:"List of solar".

	CREATE EXTERNAL TABLE hive_solr_wiki_fq ( id INT, date TIMESTAMP, title  STRING) 
	STORED BY "org.vroyer.hive.solr.SolrStorageHandler" WITH SERDEPROPERTIES ( "solr.document.mapping" = "id,date,title")
	TBLPROPERTIES ("solr.url" = "http://localhost:8983/solr/wiki.solr" );
	
	hive> SELECT * FROM hive_solr_wiki_fq WHERE title='"List of solar"';
	OK
	23755339	2010-07-17 01:50:13.998	List of solar eclipses in the 17th century
	23757234	2009-07-29 21:13:44.998	List of solar eclipses in the 6th century
	23757377	2009-07-29 21:14:34.998	List of solar eclipses in the 4th century
	23758716	2010-07-14 18:04:19.998	List of solar eclipses in the 24th century
	23755556	2011-01-17 14:04:13.998	List of solar eclipses in the 15th century
	23756230	2010-10-15 18:23:14.998	List of solar eclipses in the 10th century
	23757484	2010-07-14 17:36:05.998	List of solar eclipses in the 2nd century
	23758625	2010-07-14 18:02:17.998	List of solar eclipses in the 23rd century
	23755630	2010-05-29 21:11:59.998	List of solar eclipses in the 14th century
	23755825	2010-05-29 21:49:00.998	List of solar eclipses in the 12th century
	23757324	2010-04-22 03:15:34.998	List of solar eclipses in the 5th century
	23758509	2011-02-25 14:20:48.998	List of solar eclipses in the 22nd century
	23755450	2010-05-29 21:01:17.998	List of solar eclipses in the 16th century
	23755698	2010-05-29 21:44:19.998	List of solar eclipses in the 13th century
	23755246	2010-07-14 20:25:13.998	List of solar eclipses in the 18th century
	23757086	2009-07-29 21:08:58.998	List of solar eclipses in the 7th century
	23757446	2009-07-29 21:16:17.998	List of solar eclipses in the 3rd century
	23757589	2010-10-13 23:31:07.998	List of solar eclipses in the 1st century
	23756102	2009-07-28 23:12:51.998	List of solar eclipses in the 11th century
	23756391	2009-07-29 21:03:26.998	List of solar eclipses in the 8th century
	23754780	2010-07-14 17:59:02.998	List of solar eclipses in the 19th century
	23756290	2009-07-29 21:01:51.998	List of solar eclipses in the 9th century
	Time taken: 0.191 seconds, Fetched: 22 row(s)

You can also use a solr_query parameter to dynamically set the SOLR q parameter, in the same way Datastax supports the solr_query parameter in a CQL statement, see http://www.datastax.com/documentation/datastax_enterprise/4.5/datastax_enterprise/srch/srchCql.html. 

	CREATE EXTERNAL TABLE hive_solr_wiki_q ( id INT, date TIMESTAMP, title  STRING, solr_query STRING) 
	STORED BY "org.vroyer.hive.solr.SolrStorageHandler" WITH SERDEPROPERTIES ( "solr.document.mapping" = "id,date,title,solr_query")
	TBLPROPERTIES ("solr.url" = "http://localhost:8983/solr/wiki.solr" );
	
	hive> SELECT * FROM hive_solr_wiki_q WHERE solr_query='title:natio*';
	OK
	23739846	2011-06-08 15:21:28.998	Bolivia national football team 1999	NULL
	23745500	2010-07-25 03:44:32.998	Kenya national under-20 football team	NULL
	23743798	2011-06-08 15:24:23.998	Bolivia national football team 2000	NULL
	23748797	2011-06-08 15:27:40.998	Bolivia national football team 2002	NULL
	23748370	2011-08-18 21:02:29.998	Israel men's national inline hockey team	NULL
	23748503	2011-06-08 15:26:57.998	Bolivia national football team 2001	NULL
	23760492	2011-07-21 09:48:02.998	List of French born footballers who have played for other national teams	NULL
	Time taken: 0.09 seconds, Fetched: 7 row(s)

Because Hive currently evaluates the pushed predicates twice when residual predicate is null, previous examples have been tested with the patch https://issues.apache.org/jira/browse/HIVE-9186. Without this patch, the workaround is to add a fake (1=1) predicate to the where clause, but this may add a performance penalty if you don't have any filter task following the request to the SolrStorageHandler. You can check the execution...
 
	hive> EXPLAIN SELECT * FROM hive_solr_wiki_q WHERE solr_query='title:natio*';
	OK
	ABSTRACT SYNTAX TREE:
	  (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME hive_solr_wiki_q))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR TOK_ALLCOLREF)) (TOK_WHERE (= (TOK_TABLE_OR_COL solr_query) 'title:natio*'))))
	
	STAGE DEPENDENCIES:
	  Stage-0 is a root stage
	
	STAGE PLANS:
	  Stage: Stage-0
	    Fetch Operator
	      limit: -1
	      Processor Tree:
	        TableScan
	          alias: hive_solr_wiki_q
	          filterExpr:
	              expr: (solr_query = 'title:natio*')
	              type: boolean
	          Select Operator
	            expressions:
	                  expr: id
	                  type: int
	                  expr: date
	                  type: timestamp
	                  expr: title
	                  type: string
	                  expr: solr_query
	                  type: string
	            outputColumnNames: _col0, _col1, _col2, _col3
	            ListSink
	
	
	Time taken: 1.191 seconds, Fetched: 30 row(s)

Acknowledgements
================

Thanks to Francois Dang for his work on hive-solr (https://github.com/chimpler/hive-solr) that served as a base for this SolrStorageHandler.
