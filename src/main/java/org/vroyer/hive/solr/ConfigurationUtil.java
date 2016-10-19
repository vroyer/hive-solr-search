/**
The MIT License (MIT)

Copyright (c) 2014 Vincent ROYER

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 **/
package org.vroyer.hive.solr;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.JobConf;

import com.google.common.collect.ImmutableSet;

public class ConfigurationUtil {
	
	/*
	 * Zookeeper server URL.
	 */
	public static final String ZK_URL = "zk.url";

	/*
	 * SOLR collection identifier.
	 */
	public static final String COLLECTION_ID = "collection.id";

	/*
	 * SOLR query string.
	 */
	public static final String SOLR_QS = "solr.qs";
	
	/*
	 * solr to hive document column mapping.
	 */
	public static final String SOLR_COLUMN_MAPPING = "solr.document.mapping";
	
	/*
	 * solr to hive facet column mapping.
	 */
	public static final String SOLR_FACET_MAPPING = "solr.facet.mapping";
	
	
	/*
	 * number of document per map reduce split, default is 100,000.
	 */
	public static final String SOLR_SPLIT_SIZE = "solr.split.size";
	
		
	
	public static final Set<String> ALL_PROPERTIES = ImmutableSet.of(
			ZK_URL,
			COLLECTION_ID,
			SOLR_QS, 
			SOLR_COLUMN_MAPPING, 
			SOLR_FACET_MAPPING, 
			SOLR_SPLIT_SIZE,
			serdeConstants.LIST_COLUMNS,
			serdeConstants.LIST_COLUMN_TYPES);

	public final static String getColumnMapping(Configuration conf) {
		return conf.get(SOLR_COLUMN_MAPPING);
	}
	
	public final static String getFacetMapping(Configuration conf) {
		return conf.get(SOLR_FACET_MAPPING);
	}

	public final static String getZkUrl(Configuration conf) {
		return conf.get(ZK_URL);
	}

	public final static String getCollectionId(Configuration conf) {
		return conf.get(COLLECTION_ID);
	}

	public final static String getQs(Configuration conf) {
		return conf.get(SOLR_QS);
	}
	
	
	public final static int getSolrSplitSize(Configuration conf) {
		String value = conf.get(SOLR_SPLIT_SIZE, "100000");
		return Integer.parseInt(value);
	}
	
	
	
	public static void copySolrProperties(Properties from, JobConf to) {
    	for (String key : ALL_PROPERTIES) {
            String value = from.getProperty(key);
            if (value != null) {
            	to.set(key, value);
            }
    	}
	}

    public static void copySolrProperties(Properties from,
            Map<String, String> to) {
    	for (String key : ALL_PROPERTIES) {
            String value = from.getProperty(key);
            if (value != null) {
                to.put(key, value);
            }
    	}
    }
    	
	public static String[] getAllColumns(String columnMappingString) {
		if (!StringUtils.isBlank(columnMappingString)) {
			String[] columns = columnMappingString.split(",");
			String[] trimmedColumns = new String[columns.length];
			for (int i = 0; i < columns.length; i++) {
				trimmedColumns[i] = columns[i].trim();
			}
			return trimmedColumns;
		}
		return null;
	}
}
