package com.kensure.batchinsert.server;

import static com.kensure.batchinsert.util.ArgsConfig.ARGSMAP;
import static com.kensure.batchinsert.util.ArgsConfig.BREAKPOINT;
import static com.kensure.batchinsert.util.ArgsConfig.CALLBACK;
import static com.kensure.batchinsert.util.ArgsConfig.CALLBACKURL;
import static com.kensure.batchinsert.util.ArgsConfig.ENDTIME;
import static com.kensure.batchinsert.util.ArgsConfig.HISTORY;
import static com.kensure.batchinsert.util.ArgsConfig.INCREMENT;
import static com.kensure.batchinsert.util.ArgsConfig.MAPPING;
import static com.kensure.batchinsert.util.ArgsConfig.RUNMODE;
import static com.kensure.batchinsert.util.ArgsConfig.STARTTIME;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.kensure.batchinsert.common.ImportConfig;
import com.kensure.batchinsert.common.NodeConfig;
import com.kensure.batchinsert.common.RelactionConfig;
import com.kensure.batchinsert.node.Neo4jNodeData2HbaseServer;
import com.kensure.batchinsert.node.Neo4jNodeDataConfig;
import com.kensure.batchinsert.util.ArgsConfig;
import com.kensure.batchinsert.util.DateUtil;
import com.kensure.batchinsert.util.HbaseConfigUtil;
import com.kensure.batchinsert.util.HbaseUtil;
import com.kensure.batchinsert.util.LoadConfigUtil;
import com.kensure.batchinsert.util.Neo4jUtil;

public class RelData2Neo4j {

	private static final Log logger = LogFactory.getLog(RelData2Neo4j.class);

	private static final String BACK = "_back";

	private ImportConfig configInfo;

	private Connection conn;

	private Admin admin;

	private BatchInserter inserter;

	private String neo4jBin;

	private String neo4jConf;

	private boolean changeDataPath = false;

	private String dbms_active_database;

	private String data_source_Path;

	private String data_targe_Path;

	public RelData2Neo4j(String[] args) throws Exception {
		ArgsConfig.initArgs(args);
		LoadConfigUtil config = LoadConfigUtil.getInstance(ARGSMAP.get(MAPPING));
		configInfo = config.getConfig();
		this.neo4jBin = configInfo.getNeo4j_bin_path();
		this.neo4jConf = configInfo.getNeo4j_conf_path();
		this.dbms_active_database = Neo4jUtil.dbmsActiveDatabase(neo4jConf);
		this.data_source_Path = Neo4jUtil.dbmsDirectoriesData(neo4jConf);

		if (data_source_Path.equals("")) {
			if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
				data_source_Path = "/neo4j_data";
			} else {
				throw new RuntimeException("获取neo4j的数据目录失失败");
			}
		}
		data_targe_Path = String.format("%s/data_%s", new File(data_source_Path).getParent(),
				DateUtil.d2string("yyyyMMddHHmmss", new Date()));
		configInfo.setNeo4j_database_path(String.format("%s/databases/%s", data_source_Path, dbms_active_database));
	}

	public void insertIntoNeo4j() throws Exception {
		Set<String> tableNames = new HashSet<String>();
		try {
			tableNames.add(configInfo.getNode_index_search_table());
			tableNames.add(configInfo.getRelationship_index_search_table());
			// 初始化 Hbase
			//conn = ConnectionFactory.createConnection(LoadConfigUtil.configuration);
			conn = ConnectionFactory.createConnection(HbaseConfigUtil.getHabseConfiguration());

			admin = conn.getAdmin();
			// 初始化indexServer
			Neo4jNodeData2HbaseServer nodeIdServer = new Neo4jNodeData2HbaseServer(conn,
					new Neo4jNodeDataConfig(configInfo));
			// 初始化远行环境
			initEnvironment(nodeIdServer);
			// 初始化Neo4j文件系统
			logger.info(
					"===============加载初始化Neo4j(" + configInfo.getNeo4j_database_path() + ")文件系统...==================");
			inserter = BatchInserters.inserter(new File(configInfo.getNeo4j_database_path()));
			logger.info(
					"===============完成初始化Neo4j(" + configInfo.getNeo4j_database_path() + ")文件系统...==================");
			/**
			 * 判断是否为同一个表导入数据
			 */
			Map<String, SameTableConfig> sameTableConfig_map = sameTableConfigMap();

			TableInsertServer inserServer = new TableInsertServer(configInfo, nodeIdServer);
			// SameTableInsertServer inserServer= new
			// SameTableInsertServer(configInfo, nodeIdServer);
			// 循环处理相同的表
			/*
			 * for ( SameTableConfig sameTableConfig : sameTableConfig_map.values() ) {
			 * inserServer.insert(sameTableConfig); }
			 */
			String[] table_orders = configInfo.getNeo4j_batch_insert_table_order().split(",");
			logger.warn("");
			logger.warn("===============...数据处理...==================");
			logger.warn("");
			for (String str : table_orders) {
				if (sameTableConfig_map.containsKey(str)) {
					inserServer.insert(inserter, sameTableConfig_map.get(str));
				}
			}
			logger.warn("");
			logger.warn("==================数据处理完成（正常）==================");
			logger.warn("");
			// 保证断点信息
			saveBreakPoint();
			inserter.shutdown();
			inserter = null;

			logger.warn("");
			logger.info("==================****开始Neo4j备份****==================");
			logger.info("==================复制Neo4j数据==================");
			if(changeDataPath) {
				Neo4jUtil.cpData(data_targe_Path, data_targe_Path + BACK);
			}else {
				Neo4jUtil.cpData(data_source_Path, data_source_Path + BACK);
			}
			
			logger.info("==================****完成Neo4j备份****==================");
			if (changeDataPath) {
				if (Neo4jUtil.replaceDbmsDirectoriesData(neo4jConf, data_targe_Path)) {
					Neo4jUtil.stopNeo4j(neo4jBin);
					if (Neo4jUtil.startNeo4j(neo4jBin)) {
						HbaseUtil.deleteSnapshot(admin, tableNames);
						Neo4jUtil.deleteData(data_source_Path);
						Neo4jUtil.deleteData(data_source_Path + BACK);
					}
				}
			} else {
				Neo4jUtil.stopNeo4j(neo4jBin);
				Neo4jUtil.startNeo4j(neo4jBin);
			}

		} catch (Exception se) {
			logger.warn("");
			logger.warn("==================数据处理完成（异常）==================");
			logger.warn("");
			HbaseUtil.restoreSnapshot(admin, tableNames);
			if (null != inserter) {
				// 关闭图数据库
				inserter.shutdown();
			}
			Neo4jUtil.deleteData(data_targe_Path);
			logger.error(se);
			throw se;
		} finally {
			if (null != inserter) {
				logger.info("开始关闭数据库==================");
				// 关闭图数据库
				inserter.shutdown();
				logger.info("成功关闭数据库==================");
			}
			if (null != admin) {
				// 关闭连接
				admin.close();
			}
			if (null != conn) {
				// 关闭连接
				conn.close();
			}

		}

	}

	private void saveBreakPoint() throws IOException {
		if (ARGSMAP.get(RUNMODE).equals(INCREMENT) || ARGSMAP.get(RUNMODE).equals(HISTORY)) {
			String endTime = ARGSMAP.get(ENDTIME);
			Date eDate = new Date(Long.parseLong(endTime));
			FileUtils.writeStringToFile(new File(BREAKPOINT), DateUtil.d2string(eDate), false);
		}
	}

	/**
	 * 判断是否为同一个表导入数据
	 * 
	 * @return
	 */
	private Map<String, SameTableConfig> sameTableConfigMap() {
		Map<String, SameTableConfig> sameTableConfig_map = new HashMap<String, SameTableConfig>();
		// 遍历node
		List<NodeConfig> nodes = configInfo.getNodes();
		for (NodeConfig node : nodes) {
			String table_name = node.getTable_name();
			// 设置limit
			Integer limit = 0;
			String table_limit = node.getTable_limit();
			if (table_limit != null && table_limit.length() > 0) {
				limit = Integer.parseInt(table_limit);
			}

			// 设置同table nodes
			if (sameTableConfig_map.containsKey(table_name)) {
				sameTableConfig_map.get(table_name).addNodes(node);
				sameTableConfig_map.get(table_name).setTable_limit(limit);
			} else {
				SameTableConfig sameTableConfig = new SameTableConfig();
				sameTableConfig.addNodes(node);
				sameTableConfig.setTable_limit(limit);
				sameTableConfig.setTable_name(table_name);
				sameTableConfig_map.put(table_name, sameTableConfig);
			}
		}

		List<RelactionConfig> relactions = configInfo.getRelactions();
		for (RelactionConfig relaction : relactions) {
			String table_name = relaction.getTable_name();
			// 设置limit
			Integer limit = 0;
			String table_limit = relaction.getTable_limit();
			if (table_limit != null && table_limit.length() > 0) {
				limit = Integer.parseInt(table_limit);
			}

			// 设置同table relactions
			if (sameTableConfig_map.containsKey(table_name)) {
				sameTableConfig_map.get(table_name).addRelactions(relaction);
				sameTableConfig_map.get(table_name).setTable_limit(limit);
			} else {
				SameTableConfig sameTableConfig = new SameTableConfig();
				sameTableConfig.addRelactions(relaction);
				sameTableConfig.setTable_limit(limit);
				sameTableConfig.setTable_name(table_name);
				sameTableConfig_map.put(table_name, sameTableConfig);
			}
		}
		return sameTableConfig_map;
	}

	/**
	 * 初始化远行环境
	 * 
	 * @param nodeIdServer
	 * @throws IOException
	 */
	private void initEnvironment(Neo4jNodeData2HbaseServer nodeIdServer) throws IOException {
		// 需要清除数据
		if (ARGSMAP.get(RUNMODE).equals(HISTORY)) {
			logger.info("开始删除增量文件[" + BREAKPOINT + "]==================");
			FileUtils.deleteQuietly(new File(BREAKPOINT));
			logger.info("完成删除增量文件[" + BREAKPOINT + "]==================");
		}

		if (ARGSMAP.get(RUNMODE).equals(HISTORY) && ARGSMAP.get(STARTTIME).equals("0")
				&& "true".equalsIgnoreCase(configInfo.getNeo4j_database_beforeexc_deleted())) {
			logger.warn("");
			logger.warn("==================****开始初始化****==================");
			logger.warn("");
			logger.warn("--------------------Neo4j初始化---------------------");
			logger.warn("");
			logger.info("开始删除Neo4j原数据库内容==================");
			LoadConfigUtil.delAllFile(configInfo.getNeo4j_database_path());
			logger.info("完成删除Neo4j原数据库内容==================");
			// 删除并新建表
			logger.warn("");
			logger.warn("--------------------Hbase初始化---------------------");
			logger.warn("");
			logger.info("开始删除Hbase并新建表==================");
			logger.info("node对应ID存储到hbase中的表：" + configInfo.getNode_index_search_table());
			logger.info("relationship对应ID存储到hbase中的表：" + configInfo.getRelationship_index_search_table());
			nodeIdServer.createNeo4jIndexTable();
			logger.info("完成删除Hbase并新建表==================");
			logger.warn("");
			logger.info("==================****初始化完成****==================");
			logger.warn("");
		} else {
			logger.warn("");
			logger.info("==================****开始备份****==================");
			logger.warn("");
			/*
			 * try { TarUtil.targz(configInfo.getNeo4j_database_path(),"_" +
			 * DateUtil.d2string("yyyyMMddHHmmss", new
			 * Date(Long.parseLong(ARGSMAP.get(ENDTIME))))); } catch (Exception e) {
			 * logger.error(e); System.exit(0); }
			 */
			changeDataPath = true;
			logger.warn("--------------------Neo4j备份---------------------");
			logger.warn("");
			if (new File(data_source_Path + BACK).exists()) {
				logger.info("==================复制Neo4j数据==================");
				Neo4jUtil.cpData(data_source_Path + BACK, data_targe_Path);
			} else {
				logger.info("==================第一步：关闭Neo4j服务==================");
				Neo4jUtil.stopNeo4j(neo4jBin);
				logger.info("==================第二步：复制Neo4j数据==================");
				Neo4jUtil.cpData(data_source_Path, data_targe_Path);
				logger.info("==================第三步：打开Neo4j服务==================");
				Neo4jUtil.startNeo4j(neo4jBin);
			}
			configInfo.setNeo4j_database_path(String.format("%s/databases/%s", data_targe_Path, dbms_active_database));
			if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
				configInfo.setNeo4j_database_path(
						String.format("%s/databases/%s", data_source_Path, dbms_active_database));
			}
			logger.warn("");
			logger.warn("--------------------Hbase备份---------------------");
			logger.warn("");
			HbaseUtil.snapshot(admin, configInfo.getNode_index_search_table());
			logger.info("node对应ID存储到hbase中的表：" + configInfo.getNode_index_search_table() + "生成快照");
			HbaseUtil.snapshot(admin, configInfo.getRelationship_index_search_table());
			logger.info("relationship对应ID存储到hbase中的表：" + configInfo.getRelationship_index_search_table() + "生成快照");
			logger.warn("");
			logger.info("==================****备份完成****==================");
			logger.warn("");
		}
		logger.info("Neo4j数据库存储地址=======>：" + configInfo.getNeo4j_database_path());
	}

	public static void callback(String url_str, String callback_status) {
		try {
			logger.error("执行回调，执行结果状态为：" + ("1".equals(callback_status) ? "执行成功" : "执行失败") + "...");
			URL url = new URL(url_str + callback_status);
			URLConnection URLconnection = url.openConnection();
			HttpURLConnection httpConnection = (HttpURLConnection) URLconnection;
			int responseCode = httpConnection.getResponseCode();
			if (responseCode == HttpURLConnection.HTTP_OK) {
				logger.info("回调成功！");
			} else {
				logger.info("回调失败！");
			}
		} catch (Exception se) {
			logger.error(se);
			logger.info("回调失败！");
		}
	}

	public static void main(String[] args) {
		if (args.length == 0) {
			args = new String[] { "--mapping", "./config/config.json", "json",
					"{\"is_callback\":\"true\",\"is_increase\":\"true\"}" };
		}
		String start = DateUtil.d2string(new Date());
		try {
			RelData2Neo4j t = new RelData2Neo4j(args);
			t.insertIntoNeo4j();
			// 如果需要回调
			if ("true".equalsIgnoreCase(ARGSMAP.get(CALLBACK))) {
				logger.info("执行成功！");
				RelData2Neo4j.callback(ARGSMAP.get(CALLBACKURL), "1");
			}
		} catch (Exception se) {
			logger.error("执行失败！", se);
			// 如果需要回调
			if ("true".equalsIgnoreCase(ARGSMAP.get(CALLBACK))) {
				RelData2Neo4j.callback(ARGSMAP.get(CALLBACKURL), "0");
			}
		} finally {
			String end = DateUtil.d2string(new Date());
			logger.info("运行结束，开始时间：【" + start + "】，结束时间：【" + end + "】");
		}
	}
}
