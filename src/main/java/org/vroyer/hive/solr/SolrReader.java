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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.SimpleTimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocument;

public class SolrReader implements RecordReader<LongWritable, MapWritable> {
	public static final String READ_COLUMN_IDS_CONF_STR = "hive.io.file.readcolumn.ids";
	public static final String READ_COLUMN_NAMES_CONF_STR = "hive.io.file.readcolumn.names";
	private static final String READ_COLUMN_IDS_CONF_STR_DEFAULT = "";
	private static final String READ_ALL_COLUMNS = "hive.io.file.read.all.columns";
	private static final boolean READ_ALL_COLUMNS_DEFAULT = true;

	private static final Logger log = Logger.getLogger(SolrReader.class);

	protected static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	static {
		dateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"));
	}
	
	private SolrSplit split;
	private SolrTableCursor cursor;
	private int pos;

	private String facetMapping;

	private String[] solrColumns;
	private List<String>   hiveColNames;
	private List<TypeInfo> hiveColTypes;
	private List<ObjectInspector> rowOI = new ArrayList<ObjectInspector>();

	public SolrReader(JobConf conf, SolrSplit split) throws IOException {
		log.debug("jobConf=" + conf);

		List<Integer> readColIDs = getReadColumnIDs(conf);
		facetMapping = conf.get(ConfigurationUtil.SOLR_FACET_MAPPING);
		if (StringUtils.isBlank(facetMapping)) {
			String columnString = conf.get(ConfigurationUtil.SOLR_COLUMN_MAPPING);
			if (StringUtils.isBlank(columnString)) {
				throw new IOException("no column mapping found!");
			}
			solrColumns = ConfigurationUtil.getAllColumns(columnString);
			if (readColIDs.size() > solrColumns.length) {
				throw new IOException("read column count larger than that in column mapping string!");
			}
		} else {
			if (readColIDs.size() != 2) {
				throw new IOException("read column should be 2 with facet mapping");
			}
			solrColumns = conf.get(Constants.LIST_COLUMNS).split(",");
		}

		if (conf.get(Constants.LIST_COLUMNS) != null) {
			hiveColNames = Arrays.asList(StringUtils.split(conf.get(Constants.LIST_COLUMNS), ","));
		}

		if (conf.get(Constants.LIST_COLUMN_TYPES) != null) {
			hiveColTypes = TypeInfoUtils.getTypeInfosFromTypeString(conf.get(Constants.LIST_COLUMN_TYPES));
			for (TypeInfo ti : hiveColTypes) {
				rowOI.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(ti));
			}
		}

		this.split = split;
		SolrTable table = new SolrTable(conf);
		cursor = table.getCursor((int) split.getStart(), (int) split.getLength());
	}

	/**
	 * Returns an array of column ids(start from zero) which is set in the given
	 * parameter <tt>conf</tt>.
	 */
	public  List<Integer> getReadColumnIDs(JobConf conf) {
		String skips = conf.get(READ_COLUMN_IDS_CONF_STR, READ_COLUMN_IDS_CONF_STR_DEFAULT);
		String[] list = StringUtils.split(skips, ",");
		List<Integer> result = new ArrayList<Integer>(list.length);
		for (String element : list) {
			// it may contain duplicates, remove duplicates
			Integer toAdd = Integer.parseInt(element);
			if (!result.contains(toAdd)) {
				result.add(toAdd);
			}
		}
		return result;
	}

	
	@Override
	public void close() throws IOException {
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public MapWritable createValue() {
		return new MapWritable();
	}

	@Override
	public long getPos() throws IOException {
		return this.pos;
	}

	@Override
	public float getProgress() throws IOException {
		return split.getLength() > 0 ? pos / (float) split.getLength() : 1.0f;
	}

	@Override
	public boolean next(LongWritable keyHolder, MapWritable valueHolder)
			throws IOException {
		if (StringUtils.isBlank(facetMapping)) {
			SolrDocument doc = cursor.nextDocument();
			if (doc == null) {
				return false;
			}
			keyHolder.set(pos++);
			Object[] values = new Object[solrColumns.length];
			for (int i = 0; i < solrColumns.length; i++) {
				values[i] = doc.getFieldValue(solrColumns[i]);
			}
			setValueHolder(valueHolder, values);
		} else {
			FacetEntry facetEntry = cursor.nextFacetEntry();
			if (facetEntry == null) {
				return false;
			}
			keyHolder.set(pos++);
			setValueHolder(valueHolder, new Object[] { facetEntry.getValue(),
					facetEntry.getCount() });
		}
		return true;
	}

	public void setValueHolder(MapWritable valueHolder, Object[] values) {
		for (int i = 0; i < values.length; i++) {
			ObjectInspector oi = rowOI.get(i);

			Writable writableValue;
			if (values[i] == null) {
				writableValue = NullWritable.get();
			} else {
				log.debug("SOLR oi=" + oi + " type="
						+ values[i].getClass().getName() + " value ="
						+ values[i]);
				if ((oi != null) && (oi instanceof JavaTimestampObjectInspector)) {
					if (values[i] instanceof Date) {
						values[i] = new Timestamp(((Date) values[i]).getTime());
					} else if (values[i] instanceof String) {
						// facet date syntax : 2004-01-01T00:00:00Z
						try {
							values[i] = new Timestamp(dateFormat.parse((String) values[i]).getTime());
						} catch (ParseException e) {
							log.error("Cannot parse timestamp:" + values[i]);
						}
					}
				}
				writableValue = (Writable) ObjectInspectorUtils.copyToStandardObject(values[i], oi, ObjectInspectorCopyOption.WRITABLE);
				log.debug("value=" + writableValue + " type=" + writableValue.getClass().getName());
			}
			valueHolder.put(new Text(hiveColNames.get(i)), writableValue);
		}
	}

}
