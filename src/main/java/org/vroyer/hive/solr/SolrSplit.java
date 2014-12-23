package org.vroyer.hive.solr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class SolrSplit extends FileSplit implements InputSplit {
	
 	private static final String[] EMPTY_ARRAY = new String[] {};
	private long start, end;
	private boolean isLastSplit = false;
	
	public SolrSplit() {
	    super((Path) null, 0, 0, EMPTY_ARRAY);
	}
	
	public SolrSplit(long start, long end, Path dummyPath){
		super(dummyPath, 0, 0, EMPTY_ARRAY);
		this.start = start;
		this.end = end;
	}
	
	@Override
	  public void readFields(final DataInput input) throws IOException {
	    super.readFields(input);
	    start = input.readLong();
	    end = input.readLong();
	  }

	  @Override
	  public void write(final DataOutput output) throws IOException {
	    super.write(output);
	    output.writeLong(start);
	    output.writeLong(end);
	  }

	  public long getStart() {
	    return start;
	  }

	  public long getEnd() {
	    return end;
	  }
	  
	  public boolean isLastSplit(){
		  return this.isLastSplit;
	  }

	  public void setStart(long start) {
	    this.start = start;
	  }

	  public void setEnd(long end) {
	    this.end = end;
	  }
	  
	  public void setLastSplit(){
		  this.isLastSplit = true;
	  }

	  @Override
	  public long getLength() {
	    return end - start;
	  }

	  /* Data is remote for all nodes. */
	  @Override
	  public String[] getLocations() throws IOException {
	    return EMPTY_ARRAY;
	  }

	  @Override
	  public String toString() {
	    return String.format("SolrSplit(start=%s,end=%s)", start, end);
	  }

	
}