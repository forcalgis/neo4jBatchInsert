package com.kensure.batchinsert.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.jetty.util.StringUtil;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.kensure.batchinsert.common.ImportConfig;
import com.kensure.batchinsert.common.LabelConfig;
import com.kensure.batchinsert.common.NodeConfig;
import com.kensure.batchinsert.common.PropertyConfig;
import com.kensure.batchinsert.common.RelactionConfig;
import com.kensure.batchinsert.node.Neo4jNodeDataInfo;

public class LoadConfigUtil {

	public static final String PROPERTY_TYPE_PRIMARY = "primary";

	public static final String PROPERTY_TYPE_STARTNODE = "start";

	public static final String PROPERTY_TYPE_ENDNODE = "end";

	public static Configuration configuration;

	private static LoadConfigUtil config = null;
	private String filepath;

	/**
	 * @return the filepath
	 */
	public String getFilepath() {
		return filepath;
	}

	public LoadConfigUtil(String filepath) {
		this.filepath = filepath;
	}

	public static LoadConfigUtil getInstance() {
		return config;
	}

	public static LoadConfigUtil getInstance(String fielpath) {
		if (config != null) {
			if (!config.getFilepath().equals(fielpath)) {
				config = new LoadConfigUtil(fielpath);
				return config;
			}
			return config;
		} else {
			config = new LoadConfigUtil(fielpath);
			// 设置服务
			return config;
		}
	}

	/**
	 * @param doc
	 * @return
	 */
	public ImportConfig parseJSON() {

		ImportConfig configInfo = new ImportConfig();

		File file = new File(this.filepath);
		JSONObject jsonObject = JSONObject.fromObject(txt2String(file));

		// 基础参数
		JSONArray obj = jsonObject.getJSONArray("configs");
		for (int index = 0; index < obj.size(); index++) {
			JSONObject temp = obj.getJSONObject(index);
			String name = temp.getString("name");
			String value = temp.getString("value");
			switch (name) {
			case "hbase.zookeeper.property.clientPort":
				configInfo.setZookeeper_clientPort(value);
				break;
			case "hbase.zookeeper.quorum":
				configInfo.setZookeeper_quorum(value);
				break;
			case "neo4j.database.path":
				configInfo.setNeo4j_database_path(value);
				break;
			case "neo4j.bin.path":
				configInfo.setNeo4j_bin_path(value);
				break;
			case "neo4j.conf.path":
				configInfo.setNeo4j_conf_path(value);
				break;
			case "neo4j.database.beforeexc.deleted":
				configInfo.setNeo4j_database_beforeexc_deleted(value);
				break;
			case "node.index.search.count":
				configInfo.setNode_index_search_count(Integer.parseInt(value));
				break;
			case "node.index.search.parallel":
				configInfo.setNode_index_search_parallel(Integer.parseInt(value));
				break;
			case "node.index.search.beforeexc.deleted":
				configInfo.setNode_index_search_beforeexc_deleted(value);
				break;
			case "node.index.search.table":
				configInfo.setNode_index_search_table(value);
				break;
			case "node.index.search.family":
				configInfo.setNode_index_search_family(value);
				break;
			case "node.index.search.column":
				configInfo.setNode_index_search_column(value);
				break;
			case "node.index.search.table.partions":
				configInfo.setNode_index_search_table_partions(value);
				break;
			case "neo4j.batch.insert.table.order":
				configInfo.setNeo4j_batch_insert_table_order(value);
				break;
			case "relationship.index.search.table":
				configInfo.setRelationship_index_search_table(value);
				break;
			}
		}

		// 类型参数
		obj = jsonObject.getJSONArray("labels");
		for (int index = 0; index < obj.size(); index++) {
			JSONObject temp = obj.getJSONObject(index);
			LabelConfig labelConfig = (LabelConfig) JSONObject.toBean(temp, LabelConfig.class);
			// 添加到import Config
			configInfo.getLabels().put(labelConfig.getName(), labelConfig);
		}

		// 节点
		obj = jsonObject.getJSONArray("nodes");
		for (int index = 0; index < obj.size(); index++) {
			JSONObject temp = obj.getJSONObject(index);
			String label_name = temp.getString("label_name");
			String table_name = temp.getString("table_name");
			String table_limit = temp.getString("table_limit");
			String node_get = temp.getString("node_get");
			String node_save = temp.getString("node_save");

			NodeConfig node = new NodeConfig(label_name, table_name, table_limit, node_get, node_save);

			JSONArray properties = temp.getJSONArray("properties");
			for (int index_pro = 0; index_pro < properties.size(); index_pro++) {
				JSONObject pro = properties.getJSONObject(index_pro);
				PropertyConfig propertyConfig = (PropertyConfig) JSONObject.toBean(pro, PropertyConfig.class);
				// node的主键
				if (PROPERTY_TYPE_PRIMARY.equalsIgnoreCase(propertyConfig.getProperty_type())) {
					node.setPrimary_property(propertyConfig);
				} else {
					node.getProperties().add(propertyConfig);
				}
			}

			// 添加到import Config
			configInfo.getNodes().add(node);
		}

		obj = jsonObject.getJSONArray("relactions");
		for (int index = 0; index < obj.size(); index++) {
			JSONObject temp = obj.getJSONObject(index);
			String hbase_family = temp.getString("hbase_family");
			String hbase_field = temp.getString("hbase_field");
			String table_name = temp.getString("table_name");
			String relship_name = temp.getString("relship_name");
			String relship_displayname = temp.getString("relship_displayname");
			String table_limit = temp.getString("table_limit");
			String node_get = temp.getString("node_get");
			String node_save = temp.getString("node_save");
			RelactionConfig relaction = new RelactionConfig(hbase_family, hbase_field, table_name, relship_name,
					relship_displayname, node_get, node_save);
			relaction.setTable_limit(table_limit);

			JSONArray properties = temp.getJSONArray("properties");
			for (int index_pro = 0; index_pro < properties.size(); index_pro++) {
				JSONObject pro = properties.getJSONObject(index_pro);
				PropertyConfig propertyConfig = (PropertyConfig) JSONObject.toBean(pro, PropertyConfig.class);
				// node的主键
				if (PROPERTY_TYPE_STARTNODE.equalsIgnoreCase(propertyConfig.getProperty_type())) {
					relaction.setStart_property(propertyConfig);
				} else if (PROPERTY_TYPE_ENDNODE.equalsIgnoreCase(propertyConfig.getProperty_type())) {
					relaction.setEnd_property(propertyConfig);
				} else {
					relaction.getProperties().add(propertyConfig);
				}
			}

			// 添加到import Config
			configInfo.getRelactions().add(relaction);
		}

		// 设置hbase
		this.initConfiguration(configInfo);

		return configInfo;
	}

	public void initConfiguration(ImportConfig configInfo) {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", configInfo.getZookeeper_clientPort());
		configuration.set("hbase.zookeeper.quorum", configInfo.getZookeeper_quorum());
	}

	public ImportConfig getConfig() {
		return parseJSON();
	}

	public static String txt2String(File file) {
		String encoding = "UTF-8";
		String result = "";
		try {
			InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);// 考虑到编码格式
			BufferedReader bufferedReader = new BufferedReader(read);
			String lineTxt = null;
			while ((lineTxt = bufferedReader.readLine()) != null) {
				result = result + " " + lineTxt;
			}
			read.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return result;

	}

	// 删除指定文件夹下所有文件
	// param path 文件夹完整绝对路径
	public static boolean delAllFile(String path) {
		boolean flag = false;
		File file = new File(path);
		if (!file.exists()) {
			return flag;
		}
		if (!file.isDirectory()) {
			return flag;
		}
		String[] tempList = file.list();
		File temp = null;
		for (int i = 0; i < tempList.length; i++) {
			if (path.endsWith(File.separator)) {
				temp = new File(path + tempList[i]);
			} else {
				temp = new File(path + File.separator + tempList[i]);
			}
			if (temp.isFile()) {
				temp.delete();
			}
			if (temp.isDirectory()) {
				delAllFile(path + "/" + tempList[i]);// 先删除文件夹里面的文件
				java.io.File myFilePath = new java.io.File(path + "/" + tempList[i]);
				myFilePath.delete(); // 删除空文件夹
				flag = true;
			}
		}
		return flag;
	}

	public static String getResultData(Result result, PropertyConfig property) {
		String val = "";
		if (StringUtil.isBlank(property.getHbase_family()) && StringUtil.isBlank(property.getHbase_field())) {
			val = property.getHbase_dataformat();
		} else {
			val = Bytes.toString(result.getValue(Bytes.toBytes(property.getHbase_family()),
					Bytes.toBytes(property.getHbase_field())));
		}
		return val == null ? "" : val;
	}

	public static Long getNodeId(String labelCode, String labelNumber,
			ConcurrentHashMap<String, Neo4jNodeDataInfo> mapNodeData) {
		String key = labelCode + "_" + labelNumber;
		if (mapNodeData.containsKey(key)) {
			Neo4jNodeDataInfo nodeData = mapNodeData.get(key);
			String labelIndex = nodeData.getLabelIndex();
			if (StringUtil.isBlank(labelIndex)) {
				return null;
			} else {
				return Long.parseLong(labelIndex);
			}

		} else {
			return null;
		}

	}

	public static void main(String[] args) {
		String path = "D://StudySpc//Neo4jBatchInsert//config//config.json";
		LoadConfigUtil config = LoadConfigUtil.getInstance(path);
		ImportConfig configInfo = config.parseJSON();
		System.out.println(configInfo);
	}
}