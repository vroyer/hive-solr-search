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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;



public class SolrSerDe implements SerDe {
	
	static final String HIVE_TYPE_DOUBLE = "double";
	static final String HIVE_TYPE_FLOAT = "float";
	static final String HIVE_TYPE_BOOLEAN = "boolean";
	static final String HIVE_TYPE_BIGINT = "bigint";
	static final String HIVE_TYPE_TINYINT = "tinyint";
	static final String HIVE_TYPE_SMALLINT = "smallint";
	static final String HIVE_TYPE_INT = "int";
	static final String HIVE_TYPE_TIMESTAMP = "timestamp";
	static final String HIVE_TYPE_DATE = "date";
	

	private final MapWritable cachedWritable = new MapWritable();

	
	protected StructTypeInfo rowTypeInfo;
	protected ObjectInspector rowOI;
	protected List<String> colNames;
	protected List<Object> row;
	protected List<TypeInfo> colTypes;

	 
	private static final Logger log = Logger.getLogger(SolrSerDe.class);
	

	@Override
	public void initialize(final Configuration conf, final Properties tbl)
			throws SerDeException {
		log.debug("conf="+conf);
		log.debug("tblProperties="+tbl);
		final String facetType = tbl.getProperty(ConfigurationUtil.SOLR_FACET_MAPPING);
		final String columnString = tbl.getProperty(ConfigurationUtil.SOLR_COLUMN_MAPPING);
//		CustomLogger.info("tblProperties="+tbl);
//		CustomLogger.info(ConfigurationUtil.SOLR_FACET_MAPPING + ": " + facetType);
//		CustomLogger.info(ConfigurationUtil.SOLR_COLUMN_MAPPING + ": " + columnString);
//		CustomLogger.info(Constants.LIST_COLUMNS + ": " + tbl.getProperty(Constants.LIST_COLUMNS));

		if (StringUtils.isBlank(facetType)) {
			if (StringUtils.isBlank(columnString)) {
				throw new SerDeException("No facet mapping found, using "+ ConfigurationUtil.SOLR_COLUMN_MAPPING);
			}
			final String[] columnNamesArray = ConfigurationUtil.getAllColumns(columnString);
			colNames = Arrays.asList(columnNamesArray);
			log.debug(ConfigurationUtil.SOLR_COLUMN_MAPPING+" = " + colNames);
			row = new ArrayList<Object>(columnNamesArray.length);
//			CustomLogger.info("!!!!!!!COLUMN_NAME1: size=[" + colNames + "] : " + colNames.toString());
		} else {
			row = new ArrayList<Object>(2);
			colNames = Arrays.asList(StringUtils.split(tbl.getProperty(Constants.LIST_COLUMNS),","));
		}
		
		colTypes = TypeInfoUtils.getTypeInfosFromTypeString(tbl.getProperty(Constants.LIST_COLUMN_TYPES));
		rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
		rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
		log.debug("colNames="+colNames+" rowIO="+rowOI);
	}

	
	
	/**
	 * returns a Row as a List<Object> from the provided MapWritable.
	 */
	@Override
	public Object deserialize(Writable wr) throws SerDeException {
		if (!(wr instanceof MapWritable)) {
			throw new SerDeException("Expected MapWritable, received " + wr.getClass().getName());
		}

		final MapWritable input = (MapWritable) wr;
		final Text key = new Text();
		row.clear();
		
		for (int i = 0; i < colNames.size(); i++) {
			key.set(colNames.get(i));
			final Writable value = input.get(key);
			if (value != null && !NullWritable.get().equals(value)) {
				//parse as double to avoid NumberFormatException...
				//TODO:need more test,especially for type 'bigint'
				String hiveType = colTypes.get(i).getTypeName();
				log.debug(" value="+value+" type="+value.getClass().getName()+" hiveType="+hiveType);
				if (HIVE_TYPE_INT.equalsIgnoreCase(hiveType)) {
					row.add(Double.valueOf(value.toString()).intValue());
				} else if (SolrSerDe.HIVE_TYPE_SMALLINT.equalsIgnoreCase(hiveType)) {
					row.add(Double.valueOf(value.toString()).shortValue());
				} else if (SolrSerDe.HIVE_TYPE_TINYINT.equalsIgnoreCase(hiveType)) {
					row.add(Double.valueOf(value.toString()).byteValue());
				} else if (SolrSerDe.HIVE_TYPE_BIGINT.equalsIgnoreCase(hiveType)) {
					row.add(Long.valueOf(value.toString()));
				} else if (SolrSerDe.HIVE_TYPE_BOOLEAN.equalsIgnoreCase(hiveType)) {
					row.add(Boolean.valueOf(value.toString()));
				} else if (SolrSerDe.HIVE_TYPE_FLOAT.equalsIgnoreCase(hiveType)) {
					row.add(Double.valueOf(value.toString()).floatValue());
				} else if (SolrSerDe.HIVE_TYPE_DOUBLE.equalsIgnoreCase(hiveType)) {
					row.add(Double.valueOf(value.toString()));
				} else if (SolrSerDe.HIVE_TYPE_TIMESTAMP.equalsIgnoreCase(hiveType)) {
					row.add(((org.apache.hadoop.hive.serde2.io.TimestampWritable)value).getTimestamp());
				} else {
					row.add(value.toString());
				}
			} else {
				row.add(null);
			}
		}
		return row;
	}

	
	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return rowOI;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return MapWritable.class;
	}

	/**
	 * Not use for SOLR search !
	 */
	@Override
	public Writable serialize(final Object obj, final ObjectInspector inspector) throws SerDeException {
		final StructObjectInspector structInspector = (StructObjectInspector) inspector;
		final List<? extends StructField> fields = structInspector.getAllStructFieldRefs();
		if (fields.size() != colNames.size()) {
			throw new SerDeException(String.format(
					"Required %d columns, received %d.", colNames.size(),
					fields.size()));
		}
		
		cachedWritable.clear();
		for (int c = 0; c < fields.size(); c++) {
			StructField structField = fields.get(c);
			if (structField != null) {
				final Object field = structInspector.getStructFieldData(obj,fields.get(c));
				
				//TODO:currently only support hive primitive type
				final AbstractPrimitiveObjectInspector fieldOI = (AbstractPrimitiveObjectInspector)fields.get(c).getFieldObjectInspector();
				Writable value = (Writable)fieldOI.getPrimitiveWritableObject(field);
				log.debug(" value="+value);
				if (value == null) {
					if(PrimitiveCategory.STRING.equals(fieldOI.getPrimitiveCategory())){
						value = NullWritable.get();	
						//value = new Text("");
					}else{
						//TODO: now all treat as number
						value = new IntWritable(0);
					}
				}
				cachedWritable.put(new Text(colNames.get(c)), value);
			}
		}
		return cachedWritable;
	}

	@Override
	public SerDeStats getSerDeStats() {
		return new SerDeStats();
	}
}