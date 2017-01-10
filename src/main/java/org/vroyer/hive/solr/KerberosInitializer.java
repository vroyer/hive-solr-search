package org.vroyer.hive.solr;

import jodd.io.FileUtil;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * Initializer of Kerberos settings.
 * Settings are provided to System Properties which are used by external tools' clients
 */
public class KerberosInitializer {
    private static final String JAAS_CONF_PROPERTY = "java.security.auth.login.config";
    private static final String KRB5_CONF_PROPERTY = "java.security.krb5.conf";
    private static final String AUTH_SUBJECT_CREDS_ONLY = "javax.security.auth.useSubjectCredsOnly";

    private static final String KERBEROS_USE = "kerberos.use";
    private static final String KERBEROS_JAAS_CONFIG_PATH = "kerberos.jaas.config.path";
    private static final String KERBEROS_KRB5_CONFIG_PATH = "kerberos.krb5.config.path";
    private static final String KERBEROS_USE_SUBJECT_CREDS_ONLY = "kerberos.useSubjectCredsOnly";

    public static void init(JobConf conf) {
        boolean useKerberos = conf.getBoolean(KERBEROS_USE, false);
        CustomLogger.info("Prop [" + KERBEROS_USE + "]: " + useKerberos);
        CustomLogger.info("Prop [" + KERBEROS_JAAS_CONFIG_PATH + "]: " + conf.get(KERBEROS_JAAS_CONFIG_PATH));
        CustomLogger.info("Prop [" + KERBEROS_KRB5_CONFIG_PATH + "]: " + conf.get(KERBEROS_KRB5_CONFIG_PATH));
        CustomLogger.info("Prop [" + KERBEROS_USE_SUBJECT_CREDS_ONLY + "]: " + conf.get(KERBEROS_USE_SUBJECT_CREDS_ONLY));
        if (useKerberos) {
            try {
                String jaas = FileUtil.readString(conf.get(KERBEROS_JAAS_CONFIG_PATH));
                CustomLogger.info(jaas);
            } catch (IOException e) {
                CustomLogger.info("Can not read JAAS file: " + e);
            }
            try {
                String krb5 = FileUtil.readString(conf.get(KERBEROS_KRB5_CONFIG_PATH));
                CustomLogger.info(krb5);
            } catch (IOException e) {
                CustomLogger.info("Can not read KRB5 file: " + e);
            }
            System.setProperty(JAAS_CONF_PROPERTY, conf.get(KERBEROS_JAAS_CONFIG_PATH));
            System.setProperty(KRB5_CONF_PROPERTY, conf.get(KERBEROS_KRB5_CONFIG_PATH));
            System.setProperty(AUTH_SUBJECT_CREDS_ONLY, conf.get(KERBEROS_USE_SUBJECT_CREDS_ONLY, "false"));
        }
    }
}
