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
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class SolrInputFormat extends HiveInputFormat<LongWritable, MapWritable> {

	private static final Logger log = Logger.getLogger(SolrInputFormat.class);
 	
	
	@Override
	public RecordReader<LongWritable, MapWritable> getRecordReader(InputSplit split, JobConf conf, Reporter reporter)
			throws IOException {
		return new SolrReader(conf, (SolrSplit) split);
	}

	@Override
	public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
		log.debug("conf="+conf);
		CustomLogger.info("SolrInputFormat: JOB_CONF: \n" + conf);

		SolrTable table = new SolrTable(conf);
		CustomLogger.info("SolrInputFormat: Try to table.count().... ");
		long total = table.count();
		CustomLogger.info("SolrInputFormat: Table.count() is OK: " + total);
		int _numSplits = (numSplits < 1 || total <= numSplits) ? 1 : numSplits;
		CustomLogger.info("SolrInputFormat: _numSplits: " + _numSplits);
		final long splitSize = total / _numSplits;
		CustomLogger.info("SolrInputFormat: splitSize: " + splitSize);
		SolrSplit[] splits = new SolrSplit[_numSplits];
		final Path[] tablePaths = FileInputFormat.getInputPaths(conf);
		for (int i = 0; i < _numSplits; i++) {
			if ((i + 1) == _numSplits) {
		        splits[i] = new SolrSplit(i * splitSize, total, tablePaths[0]);
		        splits[i].setLastSplit();
		      } else {
		        splits[i] = new SolrSplit(i * splitSize, (i + 1) * splitSize, tablePaths[0]);
		      }
		}
		log.debug("splits=" + Arrays.toString(splits));
		CustomLogger.info("SolrInputFormat: splits: " + Arrays.toString(splits));
		return splits;
	}

	
}