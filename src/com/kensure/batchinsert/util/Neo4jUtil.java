package com.kensure.batchinsert.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//sed -i '/p/c\path=00000000000' t.txt
public class Neo4jUtil {

	private static final Log LOGGER = LogFactory.getLog(JavaShellUtil.class);

	public static final String dbmsDirectoriesData(String neo4jConf) {
		String shellCommand = String.format("cat %s | grep dbms.directories.data", neo4jConf);
		String rs = "";
		try {
			String result = JavaShellUtil.executeShell(shellCommand);
			String[] resultArray = result.split("\r\n");
			int resultLength = resultArray.length;
			if (resultLength > 0) {
				if (resultArray[0].equals("ok")) {
					for (int i = 1; i < resultLength; i++) {
						String[] dbmsDirectoriesDataArray = resultArray[i].split("=");
						if (dbmsDirectoriesDataArray.length == 2) {
							if (dbmsDirectoriesDataArray[0].trim().equals("dbms.directories.data")) {
								rs = dbmsDirectoriesDataArray[1].trim();
								break;
							}
						}
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error(e);
		}
		LOGGER.info(String.format("neo4j配置文件中的内容-->[dbms.directories.data:%s]", rs));
		return rs;
	}

	public static final String dbmsActiveDatabase(String neo4jConf) {
		String shellCommand = String.format("cat %s | grep dbms.active_database", neo4jConf);
		String rs = "graph.db";
		try {
			String result = JavaShellUtil.executeShell(shellCommand);
			String[] resultArray = result.split("\r\n");
			int resultLength = resultArray.length;
			if (resultLength > 0) {
				if (resultArray[0].equals("ok")) {
					for (int i = 1; i < resultLength; i++) {
						String[] dbmsActiveDatabaseArray = resultArray[i].split("=");
						if (dbmsActiveDatabaseArray.length == 2) {
							if (dbmsActiveDatabaseArray[0].trim().equals("dbms.active_database")) {
								rs = dbmsActiveDatabaseArray[1].trim();
								break;
							}
						}
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error(e);
		}
		LOGGER.info(String.format("neo4j配置文件中的内容-->[dbms.active_database:%s]", rs));
		return rs;
	}

	public static final boolean startNeo4j(String neo4jBin) {
		String shellCommand = String.format("%s start", neo4jBin);
		try {
			String result = JavaShellUtil.executeShell(shellCommand);
			String[] resultArray = result.split("\r\n");
			int resultLength = resultArray.length;
			if (resultLength > 0) {
				if (resultArray[0].equals("ok")) {
					return true;
				}
			}
		} catch (Exception e) {
			LOGGER.error(e);
		}
		return false;
	}

	public static final boolean stopNeo4j(String neo4jBin) {
		String shellCommand = String.format("%s stop", neo4jBin);
		try {
			String result = JavaShellUtil.executeShell(shellCommand);
			String[] resultArray = result.split("\r\n");
			int resultLength = resultArray.length;
			if (resultLength > 1) {
				if (resultArray[0].equals("ok")) {
					return true;
				}
			}
		} catch (Exception e) {
			LOGGER.error(e);
		}
		return false;
	}

	public static final boolean replaceDbmsDirectoriesData(String neo4jConf, String dbmsDirectoriesData) {
		String shellCommand = String.format("sed -i '/dbms.directories.data=/c\\dbms.directories.data=%s' %s",
				dbmsDirectoriesData, neo4jConf);
		try {
			String result = JavaShellUtil.executeShell(shellCommand);
			String[] resultArray = result.split("\r\n");
			int resultLength = resultArray.length;
			if (resultLength == 1) {
				return true;
			}
			if (resultLength > 1) {
				if (resultArray[0].equals("ok")) {
					return true;
				}
			}
		} catch (Exception e) {
			LOGGER.error(e);
		}
		return false;
	}

	public static final boolean cpData(String source, String tager) {
		String shellCommand = String.format("cp -r %s %s", source, tager);
		try {
			String result = JavaShellUtil.executeShell(shellCommand);
			String[] resultArray = result.split("\r\n");
			int resultLength = resultArray.length;
			if (resultLength == 1) {
				return true;
			}
			if (resultLength > 1) {
				if (resultArray[0].equals("ok")) {
					return true;
				}
			}
		} catch (Exception e) {
			LOGGER.error(e);
		}
		return false;
	}
	
	public static final boolean moveData(String source, String tager) {
		String shellCommand = String.format("mv -f %s %s", source, tager);
		try {
			String result = JavaShellUtil.executeShell(shellCommand);
			String[] resultArray = result.split("\r\n");
			int resultLength = resultArray.length;
			if (resultLength == 1) {
				return true;
			}
			if (resultLength > 1) {
				if (resultArray[0].equals("ok")) {
					return true;
				}
			}
		} catch (Exception e) {
			LOGGER.error(e);
		}
		return false;
	}

	public static final boolean deleteData(String source) {
		String shellCommand = String.format("rm -fr %s", source);
		try {
			String result = JavaShellUtil.executeShell(shellCommand);
			String[] resultArray = result.split("\r\n");
			int resultLength = resultArray.length;
			if (resultLength == 1) {
				return true;
			}
			if (resultLength > 1) {
				if (resultArray[0].equals("ok")) {
					return true;
				}
			}
		} catch (Exception e) {
			LOGGER.error(e);
		}
		return false;
	}

	public static void main(String[] args) {
		String neo4jConf = "/usr/local/share/neo4j/conf/neo4j.conf";
		String neo4jBin = "/usr/local/share/neo4j/bin/neo4j";
		// System.out.println(Neo4jUtil.dbmsDirectoriesData(neo4jConf));
		// System.out.println(Neo4jUtil.dbmsActiveDatabase(neo4jConf));
		// System.out.println(Neo4jUtil.stopNeo4j(neo4jBin));
		// System.out.println(Neo4jUtil.startNeo4j(neo4jBin));
		// cp -r /home/xdk/neo4j_data/data /home/xdk/neo4j_data/data_01
		// Neo4jUtil.cpData("/home/xdk/neo4j_data/data",
		// "/home/xdk/neo4j_data/data_03");
		Neo4jUtil.replaceDbmsDirectoriesData(neo4jConf, "/home/xdk/neo4j_data/data_01");

	}

}
