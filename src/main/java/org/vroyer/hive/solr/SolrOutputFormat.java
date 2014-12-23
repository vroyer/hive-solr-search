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
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;

public class SolrOutputFormat implements HiveOutputFormat<NullWritable, Writable>{

        @Override
        public RecordWriter getHiveRecordWriter(JobConf conf,
                      Path finalOutPath,
                      Class<? extends Writable> valueClass,
                      boolean isCompressed,
                      Properties tableProperties,
                      Progressable progress) throws IOException {
                return new SolrWriter(conf);

        }

        @Override
        public void checkOutputSpecs(FileSystem arg0, JobConf conf)
                        throws IOException {
                // TODO Auto-generated method stub

        }

        @Override
        public org.apache.hadoop.mapred.RecordWriter<NullWritable, Writable> getRecordWriter(
                        FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
                        throws IOException {
                throw new RuntimeException("Error: Hive should not invoke this method.");
        }

}