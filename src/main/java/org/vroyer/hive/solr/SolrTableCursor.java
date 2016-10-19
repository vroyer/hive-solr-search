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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RangeFacet;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class SolrTableCursor {
	SolrTable table ;
	
	private SolrServer server;
	private int start;
	private int count;
	private int solrSplitSize;
	
 
	private SolrDocumentList buffer;
	private List<FacetEntry> facets = new ArrayList<FacetEntry>();
	private int pos = 0;
	private long numFound;

	private static final Logger log = Logger.getLogger(SolrTableCursor.class);
	
	
	SolrTableCursor(SolrTable table, int start, int count, int solrSplitSize) throws IOException {
		this.table = table;
        server = SolrServerFactory.getInstance().createCloudServer(table.zkUrl, table.collectionId);
        this.start = start;
        this.count = count;
        this.solrSplitSize = solrSplitSize;
        fetchNextDocumentChunk();
	}

	private void fetchNextDocumentChunk() throws IOException {
        SolrQuery query = new SolrQuery();
        query.setStart(start);
        
        if (table.facetType == null) {
        	query.setRows( Math.min(count,this.solrSplitSize));
        	for(String s:table.fields) {
        		// Don't push solr_query, this is an internal column when a cassandra table is indexed by SOLR.
        		if (!s.equalsIgnoreCase("solr_query")) {
        			query.addField(s);
        		}
        	}
        } else {
        	query.setRows(0);
        }
        
		List<NameValuePair> params = URLEncodedUtils.parse(table.qs, Charset.forName("UTF-8"));
		for(NameValuePair nvp: params) {
			query.set(nvp.getName(), nvp.getValue());
		}
		if (table.fq.length() > 0) {
			query.set("fq",table.fq.toString());
		}
		if (table.q.length() > 0) {
			query.set("q", table.q.toString());
		}
		if (query.get("q") == null) {
			query.set("q", "*:*");
		}
		
		if (log.isInfoEnabled()) {
			StringBuffer sb = new StringBuffer("");
			Map<String,String[]> map = query.toMultiMap(query.toNamedList());
			for(String s: map.keySet()) {
				sb.append(s).append('=').append(Arrays.toString(map.get(s))).append(' ');
			}
			log.info("SOLR request: "+sb.toString());
		}
		
        
		QueryResponse response;
		try {
			response = server.query(query);
		} catch (SolrServerException e) {
			throw new IOException(e);
		}
		
		if (table.facetType != null) {
			if (table.facetType.equalsIgnoreCase("ranges")) {
				List<RangeFacet.Count> counts = response.getFacetRanges().get(0).getCounts();
				for(RangeFacet.Count rfc : counts) {
					facets.add(new FacetEntry(rfc.getValue(), rfc.getCount()));
				}
			} else if (table.facetType.equalsIgnoreCase("fields")) {
				List<FacetField.Count> counts = response.getFacetFields().get(0).getValues();
				for(FacetField.Count  rfc : counts) {
					facets.add(new FacetEntry(rfc.getName(), rfc.getCount()));
				}
			} else if (table.facetType.equalsIgnoreCase("queries")) {
				Map<String,Integer> queries = response.getFacetQuery();
				for(String k : queries.keySet()) {
					facets.add(new FacetEntry(k, queries.get(k)));
				}
			} 
		} else {
			buffer = response.getResults();
			numFound = buffer.getNumFound();
			log.info("SOLR response numFound="+buffer.getNumFound());
			start += buffer.size();
			count -= buffer.size();
		}
		pos = 0;
	}
	
	SolrDocument nextDocument() throws IOException {
		if (pos >= buffer.size()) {
			if (count > 0) {
				fetchNextDocumentChunk();
			}
			if (pos >= buffer.size()) {
				return null;
			}
		}
		return buffer.get(pos++);
	}
	
	FacetEntry nextFacetEntry() throws IOException {
		if (pos >= facets.size()) {
			return null;
		}
		return facets.get(pos++);
	}
	
	
	long getNumFound() {
		return numFound;
	}
	
}