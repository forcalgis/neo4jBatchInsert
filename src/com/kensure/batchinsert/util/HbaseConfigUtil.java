package com.kensure.batchinsert.util;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;

public class HbaseConfigUtil {

	private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
	private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
	private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";

	private static Configuration conf = null;
	private static String krb5File = null;
	private static String userName = null;
	private static String userKeytabFile = null;

	public static Configuration getHabseConfiguration() throws IOException {
		if (conf == null) {
			conf = HBaseConfiguration.create();
			String userdir = System.getProperty("user.dir") + File.separator
					+ "config" + File.separator;
			conf.addResource(new Path(userdir + "core-site.xml"), false);
			conf.addResource(new Path(userdir + "hdfs-site.xml"), false);
			conf.addResource(new Path(userdir + "hbase-site.xml"), false);

			if (User.isHBaseSecurityEnabled(conf)) {
				userName = "hbkx";
				userKeytabFile = userdir + "user.keytab";
				krb5File = userdir + "krb5.conf";
				System.out.println(krb5File);

				/*
				 * if need to connect zk, please provide jaas info about zk. of
				 * course, you can do it as below:
				 * System.setProperty("java.security.auth.login.config",
				 * confDirPath + "jaas.conf"); but the demo can help you more :
				 * Note: if this process will connect more than one zk cluster,
				 * the demo may be not proper. you can contact us for more help
				 */
				LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME,
						userName, userKeytabFile);
				LoginUtil.setZookeeperServerPrincipal(
						ZOOKEEPER_SERVER_PRINCIPAL_KEY,
						ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
				LoginUtil.login(userName, userKeytabFile, krb5File, conf);
			}
		}
		return conf;
	}

}
