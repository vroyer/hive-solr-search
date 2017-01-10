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
import java.util.Collection;

import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import sun.security.provider.ConfigFile;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

public class SolrTable {
	private SolrServer server;
	
	protected int solrSplitSize;
	protected String[] fields;
	protected String facetType;
	protected String zkUrl;
	protected String collectionId;
	protected String qs;
	protected StringBuilder fq = new StringBuilder();
	protected StringBuilder q = new StringBuilder();
	
	private Collection<SolrInputDocument> outputBuffer;
	
	private static final Logger log = Logger.getLogger(SolrTable.class);

	
	public SolrTable(JobConf conf) {

		CustomLogger.info("Init kerberos settings..");
		KerberosInitializer.init(conf);

		/*try {
			CustomLogger.info("Try to KINIT");
			Process process = Runtime.getRuntime().exec("kinit -kt /dmp/kerberos_config/usrsolr.keytab usrsolr");
			int status = process.waitFor();
			CustomLogger.info("SUCCESS Try to KINIT: status=" + status);
		} catch (Exception e) {
			CustomLogger.info("ERROR Try to KINIT: " + e);
		}*/

		/*UserGroupInformation.setConfiguration(conf);
		try {
			CustomLogger.info("Try to UserGroupInformation.loginUserFromKeytabAndReturnUGI");
			UserGroupInformation.loginUserFromKeytabAndReturnUGI("usrsolr@DMP.COM", "/dmp/kerberos_config/usrsolr.keytab");
			CustomLogger.info("SUCCESS Try to UserGroupInformation.loginUserFromKeytabAndReturnUGI");
		} catch (IOException e) {
			CustomLogger.info("ERROR Try to UserGroupInformation.loginUserFromKeytabAndReturnUGI: " + e);
		}

		LoginContext loginContext = null;
		Subject subject = new Subject();
		try {
			CustomLogger.info("Try to ST.LoginContext");
			Configuration configuration = new Configuration() {
				@Override
				public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
					return new AppConfigurationEntry[0];
				}
			};
			loginContext = newLoginContext("solr", subject, );
			CustomLogger.info("Try to ST.LoginContext.login");
			loginContext.login();
			CustomLogger.info("SUCCESS Try to ST.LoginContext.login");
		} catch (LoginException e) {
			CustomLogger.info("ERROR Try to ST.LoginContext.login: " + e);
		}*/

		String filterExprSerialized = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
		if (filterExprSerialized != null) {
			ExprNodeDesc filterExpr = Utilities.deserializeExpression(filterExprSerialized);
			log.debug("filterExpr="+filterExpr.getExprString());
			SolrStorageHandler.buildQuery(filterExpr,fq,q);
		}
		
        this.zkUrl = ConfigurationUtil.getZkUrl(conf);
		this.collectionId = ConfigurationUtil.getCollectionId(conf);
		this.qs = ConfigurationUtil.getQs(conf);
		this.fields = ConfigurationUtil.getAllColumns(conf.get(ConfigurationUtil.SOLR_COLUMN_MAPPING));
        this.facetType = conf.get(ConfigurationUtil.SOLR_FACET_MAPPING);
        log.info("zk.url="+zkUrl+" solr.collection="+collectionId+" solr.qs="+qs+" fq="+fq+" q="+q);

		CustomLogger.info("zk.url="+zkUrl+" solr.collection="+collectionId+" solr.qs="+qs+" fq="+fq+" q="+q);
		CustomLogger.info("JobConf user: " + conf.getUser());

        
        this.solrSplitSize = ConfigurationUtil.getSolrSplitSize(conf);
        this.outputBuffer = new ArrayList<SolrInputDocument>(solrSplitSize);
        this.server = SolrServerFactory.getInstance().createCloudServer(zkUrl, collectionId);
	}

	private static LoginContext newLoginContext(String appName, Subject subject, javax.security.auth.login.Configuration loginConf) throws LoginException {
		Thread t = Thread.currentThread();
		ClassLoader oldCCL = t.getContextClassLoader();
		t.setContextClassLoader(UserGroupInformation.HadoopLoginModule.class.getClassLoader());

		LoginContext var5;
		try {
			var5 = new LoginContext(appName, subject, (CallbackHandler)null, loginConf);
		} finally {
			t.setContextClassLoader(oldCCL);
		}

		return var5;
	}

	public void save(SolrInputDocument doc) throws IOException {
		outputBuffer.add(doc);
		if (outputBuffer.size() >= solrSplitSize) {
			flush();
		}
	}
	
	public void flush() throws IOException {
		try {
			if (!outputBuffer.isEmpty()) {
				server.add(outputBuffer);
				outputBuffer.clear();
			}
		} catch (SolrServerException e) {
			throw new IOException(e);
		}
	}

	public long count() throws IOException {
		return getCursor( 0, 0).getNumFound();
	}

	public SolrTableCursor getCursor(int start, int count) throws IOException {
		return new SolrTableCursor(this, start, (facetType==null) ? count : 1, solrSplitSize);
	}
	
	public void drop() throws IOException{
		try {
			server.deleteByQuery("*:*");
			server.commit();
		} catch (SolrServerException e) {
			throw new IOException(e);
		}
	}

	public void commit() throws IOException {
		try {
			flush();
			server.commit();
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}

	public void rollback() throws IOException {
		try {
			outputBuffer.clear();
			server.rollback();
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}

}
