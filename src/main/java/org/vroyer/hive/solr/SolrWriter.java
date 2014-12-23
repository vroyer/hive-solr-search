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
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.SimpleTimeZone;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;

public class SolrWriter implements RecordWriter {
	
	// SOLR Date format 
	protected static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+SSSS'Z'");
	static {
		dateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"));
	}
	
	private static final Logger log = Logger.getLogger(SolrWriter.class);
	
	
	private SolrTable table;
	
	public SolrWriter(JobConf conf) {
		this.table = new SolrTable(conf);
	}

	@Override
	public void close(boolean abort) throws IOException {
		if (!abort) {
			table.commit();
		} else {
			table.rollback();
		}
	}

	@Override
	public void write(Writable w) throws IOException {
		MapWritable map = (MapWritable) w;
		SolrInputDocument doc = new SolrInputDocument();
		for (final Map.Entry<Writable, Writable> entry : map.entrySet()) {
			String key = entry.getKey().toString();
			
			if (entry.getValue() instanceof TimestampWritable) {
				Timestamp t = ((TimestampWritable)entry.getValue()).getTimestamp();
				doc.setField(key, dateFormat.format( new Date(t.getTime()) ));
			} else if (entry.getValue() instanceof ShortWritable) {
			    doc.setField(key, ((ShortWritable)entry.getValue()).get());
			} else {
				doc.setField(key, entry.getValue().toString());
			}
			
		}
		log.debug("doc:"+doc.toString());
		table.save(doc);
	}

	private Object getObjectFromWritable(Writable w) {
		if (w instanceof IntWritable) {
			// int
			return ((IntWritable) w).get();
		} else if (w instanceof ShortWritable) {
			// short
			return ((ShortWritable) w).get();
		} else if (w instanceof ByteWritable) {
			// byte
			return ((ByteWritable) w).get();
		} else if (w instanceof BooleanWritable) {
			// boolean
			return ((BooleanWritable) w).get();
		} else if (w instanceof LongWritable) {
			// long
			return ((LongWritable) w).get();
		} else if (w instanceof FloatWritable) {
			// float
			return ((FloatWritable) w).get();
		} else if (w instanceof DoubleWritable) {
			// double
			return ((DoubleWritable) w).get();
		}else if (w instanceof NullWritable) {
			//null
			return null;
		} else {
			// treat as string
			return w.toString();
		}
	}
}
