package org.vroyer.hive.solr;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.solr.client.solrj.impl.*;
import org.ietf.jgss.GSSManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

public final class SolrServerFactory {
    private final static Logger LOG = LoggerFactory.getLogger(SolrServerFactory.class);

    private Map<String, CloudSolrServer> urlToCloudSolrServer = new ConcurrentHashMap<String, CloudSolrServer>();
    private static SolrServerFactory instance = new SolrServerFactory();

    public static SolrServerFactory getInstance() {
        return instance;
    }

    private SolrServerFactory() {
    }

    public synchronized CloudSolrServer createCloudServer(String zkHost, String collectionId) {
        CustomLogger.info("urlToCloudSolrServer.containsKey(" + collectionId + "): " + urlToCloudSolrServer.containsKey(collectionId));
        if (!urlToCloudSolrServer.containsKey(collectionId)) {
            HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());

//
//            GSSManager manager = GSSManager.getInstance();
//
//            UserGroupInformation.setConfiguration(conf);
//            UserGroupInformation.loginUserFromKeytabAndReturnUGI("usrsolr@DMP.COM", "/home/dmp/kerberos_config/hdfswriter.keytab");

            CloudSolrServer server = new CloudSolrServer(zkHost);
            server.setDefaultCollection(collectionId);
            urlToCloudSolrServer.put(collectionId, server);
            LOG.info("Cloud solr server client was created with zkHost={}, collectionId={}", zkHost, collectionId);
            CustomLogger.info("Cloud solr server client was created with zkHost=" + zkHost + ", collectionId=" + collectionId);
            try {
                server.connect();
                CustomLogger.info("CONNECT to SOLR server IS OK: ");
            }catch (RuntimeException e) {
                CustomLogger.info("ERROR::: CAN NOT CONNECT to SOLR server: " + e);
            }
        }
        return urlToCloudSolrServer.get(collectionId);
    }
}