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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;

public class SolrStorageHandler implements HiveStorageHandler,HiveStoragePredicateHandler {
	
	private static final Log log = LogFactory.getLog(SolrStorageHandler.class.getName());
	
	private Configuration conf = null;
	
	public SolrStorageHandler() {
	}

	@Override
	public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
		Properties properties = tableDesc.getProperties();
		ConfigurationUtil.copySolrProperties(properties, jobProperties);
	}

	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		return SolrInputFormat.class;
	}

	@Override
	public HiveMetaHook getMetaHook() {
		return new DummyMetaHook();
	}

	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		return SolrOutputFormat.class;
	}

	@Override
	public Class<? extends SerDe> getSerDeClass() {
		return SolrSerDe.class;
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	private class DummyMetaHook implements HiveMetaHook {

		@Override
		public void commitCreateTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void commitDropTable(Table tbl, boolean deleteData)
				throws MetaException {
			boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
			if (deleteData && isExternal) {
				// nothing to do...
			} else if(deleteData && !isExternal) {
				String zkUrl = tbl.getParameters().get(ConfigurationUtil.ZK_URL);
				String collectionId = tbl.getParameters().get(ConfigurationUtil.COLLECTION_ID);
	            SolrServer server = SolrServerFactory.getInstance().createCloudServer(zkUrl, collectionId);
	            try {
					server.deleteByQuery("*:*");
					server.commit();
				} catch (SolrServerException e) {
					throw new MetaException(e.getMessage());
				} catch (IOException e) {
					throw new MetaException(e.getMessage());
				}
			}
		}

		@Override
		public void preCreateTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void preDropTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void rollbackCreateTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void rollbackDropTable(Table tbl) throws MetaException {
			// nothing to do...
		}

	}

	@Override
	public void configureInputJobProperties(TableDesc tableDescription, Map<String, String> jobProperties) {
		Properties properties = tableDescription.getProperties();
		ConfigurationUtil.copySolrProperties(properties, jobProperties);
	}

	@Override
	public void configureOutputJobProperties(TableDesc tableDescription, Map<String, String> jobProperties) {
		Properties properties = tableDescription.getProperties();
		ConfigurationUtil.copySolrProperties(properties, jobProperties);
	}
	
	@Override
	public void configureJobConf(TableDesc tableDescription, JobConf config) {
		Properties properties = tableDescription.getProperties();
		ConfigurationUtil.copySolrProperties(properties, config);
	}

	

	@Override
	public HiveAuthorizationProvider getAuthorizationProvider()
			throws HiveException {
		return new DefaultHiveAuthorizationProvider();
	}


	
	@Override
	public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer, ExprNodeDesc predicate) {
		SolrSerDe serDe = (SolrSerDe)deserializer;
		log.debug(ConfigurationUtil.SOLR_COLUMN_MAPPING+"="+serDe.colNames+" predicate columns="+predicate.getCols());
		boolean found = false;
		for(String s:predicate.getCols()) {
			if (serDe.colNames.contains(s)) found=true;
		}
		if (!found) return null;
		log.debug(" predicate="+predicate.getExprString());

		
		DecomposedPredicate dp = new DecomposedPredicate();
		if (pushDownFilter(serDe.colNames, predicate, dp)) {
			log.debug("decomposed pushed: "+dp.pushedPredicate.getExprString());
			log.debug("decomposed residual: "+((dp.residualPredicate == null) ? null : dp.residualPredicate.getExprString()));
			return dp;
		}
		return null;
	}
	
	protected static void dumpFilterExpr(ExprNodeDesc node) {
		if (node != null) {
			log.debug("dump: " + node.getClass().getName()+" name="+node.getName()+" expr="+node.getExprString()+ "[ ");
			if (node instanceof ExprNodeGenericFuncDesc) {
				log.debug(" func="+ ((ExprNodeGenericFuncDesc)node).getGenericUDF() );
			}
			List<ExprNodeDesc> children = node.getChildren();
			if (children != null) {
				for (ExprNodeDesc child: children) {
					if (child != null) dumpFilterExpr(child);
					log.debug(",");
				}
			}
			log.debug("]");
		}
	}
	
	protected boolean pushDownFilter(List<String> solrColumns, ExprNodeDesc input, DecomposedPredicate dp) {
		if (input instanceof ExprNodeGenericFuncDesc) {
			ExprNodeGenericFuncDesc inputFunc = (ExprNodeGenericFuncDesc)input;
			ExprNodeDesc inputLeft = inputFunc.getChildren().get(0);
			ExprNodeDesc inputRight = inputFunc.getChildren().get(1);
			DecomposedPredicate dpLeft = new DecomposedPredicate();
			DecomposedPredicate dpRight = new DecomposedPredicate();
			
			if ((inputFunc.getGenericUDF() instanceof GenericUDFOPOr) || (inputFunc.getGenericUDF() instanceof GenericUDFOPAnd))  {
				boolean pushLeft=pushDownFilter(solrColumns, inputFunc.getChildren().get(0), dpLeft);
				boolean pushRight=pushDownFilter(solrColumns, inputFunc.getChildren().get(1), dpRight);
				if (pushLeft && pushRight && (dpLeft.residualPredicate == null) && (dpRight.residualPredicate == null)) {
					// ful push down
					dp.pushedPredicate = (ExprNodeGenericFuncDesc)input;
					dp.residualPredicate = null;
					return true;
				} 
				if (inputFunc.getGenericUDF() instanceof GenericUDFOPAnd) {
					if ((dpLeft.pushedPredicate!=null) && (dpRight.pushedPredicate!=null)) {
						List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(2);
						children.add(dpLeft.pushedPredicate);
						children.add(dpRight.pushedPredicate);
						dp.pushedPredicate = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,FunctionRegistry.getGenericUDFForAnd(), children);
					} else if (dpLeft.pushedPredicate!=null) { 
						dp.pushedPredicate = dpLeft.pushedPredicate;
					} else if (dpRight.pushedPredicate!=null) {
						dp.pushedPredicate = dpRight.pushedPredicate;
					} else {
						dp.pushedPredicate = null;
						dp.residualPredicate = (ExprNodeGenericFuncDesc)input;
						return false;
					}
					if ((dpLeft.residualPredicate!=null)&&(dpRight.residualPredicate!=null)) {
						List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(2);
						children.add(dpLeft.residualPredicate);
						children.add(dpRight.residualPredicate);
						dp.pushedPredicate = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,FunctionRegistry.getGenericUDFForAnd(), children);
					} else if (dpLeft.residualPredicate!=null) {
						dp.residualPredicate =  dpLeft.residualPredicate;
					} else if (dpRight.residualPredicate!=null) {
						dp.residualPredicate =  dpRight.residualPredicate;
					} else {
						dp.residualPredicate = null;
					}
					return true;
				}
			} else if (inputFunc.getGenericUDF() instanceof GenericUDFOPEqual)  {
				if ((isSolrColumn(solrColumns, inputLeft) && isConstant(inputRight)) ||
				    (isSolrColumn(solrColumns, inputRight) && isConstant(inputLeft))) {
					dp.pushedPredicate = (ExprNodeGenericFuncDesc)input;
					dp.residualPredicate = null;
					return true;
				}
			}
			dp.residualPredicate = (ExprNodeGenericFuncDesc) input;
		}
		dp.pushedPredicate = null;
		return false;
	}
	
	protected boolean isSolrColumn(List<String> solrColumns, ExprNodeDesc operand) {
		if ((operand==null) || (operand instanceof ExprNodeColumnDesc)) {
			String colName = ((ExprNodeColumnDesc)operand).getColumn();
			if (colName.equalsIgnoreCase("solr_query") || solrColumns.contains(colName)) return true;
		}
		return false;
	}
	
	protected boolean isConstant(ExprNodeDesc operand) {
		return (operand instanceof ExprNodeConstantDesc);
	}
	
	protected static void buildQuery(ExprNodeDesc input, StringBuilder fq, StringBuilder q) {
		if (input instanceof ExprNodeGenericFuncDesc) {
			ExprNodeGenericFuncDesc inputFunc = (ExprNodeGenericFuncDesc)input;
			ExprNodeDesc inputLeft = inputFunc.getChildren().get(0);
			ExprNodeDesc inputRight = inputFunc.getChildren().get(1);
			
			if ((inputFunc.getGenericUDF() instanceof GenericUDFOPOr) ||  (inputFunc.getGenericUDF() instanceof GenericUDFOPAnd)) {
				StringBuilder fqLeft = new StringBuilder();
				StringBuilder fqRight = new StringBuilder();
				buildQuery(inputLeft,fqLeft,q);
				buildQuery(inputRight,fqRight,q);
				if (fqLeft.length()>0 && fqRight.length()>0) {
					fq.append("(").append(fqLeft).append(") ");
					fq.append( (inputFunc.getGenericUDF() instanceof GenericUDFOPOr) ? "OR" : "AND");
					fq.append("(").append(fqRight).append(") ");
				} else if (fqLeft.length()>0) {
					fq.append(fqLeft);
				} else if (fqRight.length()>0) {
					fq.append(fqRight);
				}
					
			} else if (inputFunc.getGenericUDF() instanceof GenericUDFOPEqual) {
				if ((inputLeft instanceof ExprNodeColumnDesc) && (inputRight instanceof ExprNodeConstantDesc)) {
					if (((ExprNodeColumnDesc)inputLeft).getColumn().equalsIgnoreCase("solr_query")) {
						q.append(((ExprNodeConstantDesc)inputRight).getValue().toString());
					} else {
						fq.append(((ExprNodeColumnDesc)inputLeft).getColumn());
						fq.append(":");
						fq.append(((ExprNodeConstantDesc)inputRight).getValue().toString());
					}
				} else {
					if (((ExprNodeColumnDesc)inputRight).getColumn().equalsIgnoreCase("solr_query")) {
						q.append(((ExprNodeConstantDesc)inputLeft).getValue());
					} else {
						fq.append(((ExprNodeColumnDesc)inputRight).getColumn());
						fq.append(":");
						fq.append(((ExprNodeConstantDesc)inputLeft).getValue().toString());
					}
				}
			}
		} 
		return;
	}
}
