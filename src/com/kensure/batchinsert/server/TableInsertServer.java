package com.kensure.batchinsert.server;

import static com.kensure.batchinsert.util.ArgsConfig.ARGSMAP;
import static com.kensure.batchinsert.util.ArgsConfig.ENDTIME;
//import static com.kensure.batchinsert.util.ArgsConfig.INCREMENT;
//import static com.kensure.batchinsert.util.ArgsConfig.RUNMODE;
import static com.kensure.batchinsert.util.ArgsConfig.STARTTIME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.jetty.util.StringUtil;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.unsafe.batchinsert.BatchInserter;

import com.kensure.batchinsert.common.ImportConfig;
import com.kensure.batchinsert.common.LabelConfig;
import com.kensure.batchinsert.common.NodeConfig;
import com.kensure.batchinsert.common.PropertyConfig;
import com.kensure.batchinsert.common.RelactionConfig;
import com.kensure.batchinsert.node.Neo4jNodeData2HbaseServer;
import com.kensure.batchinsert.util.DateUtil;
import com.kensure.batchinsert.util.IDCardUtil;
import com.kensure.batchinsert.util.LoadConfigUtil;

public class TableInsertServer {

	private static final Log logger = LogFactory.getLog(TableInsertServer.class);

	private Neo4jNodeData2HbaseServer nodeIdServer;

	private ImportConfig configInfo;

	private int counts = 0;

	public TableInsertServer(ImportConfig configInfo, Neo4jNodeData2HbaseServer nodeIdServer) {
		this.configInfo = configInfo;
		this.nodeIdServer = nodeIdServer;
	}

	public void insert(BatchInserter inserter, SameTableConfig sameTableConfig) throws IOException {
		String message = "      nodes \t [%s] \t relactions [%s] \t 开始插入数据 \t [%s--%s]";
		counts = 0;
		long start = System.currentTimeMillis();
		Connection conn = nodeIdServer.getConnection();

		Table table = conn.getTable(TableName.valueOf(sameTableConfig.getTable_name()));

		Scan scan = new Scan();
		scan.setTimeRange(Long.parseLong(ARGSMAP.get(STARTTIME)), Long.parseLong(ARGSMAP.get(ENDTIME)));

		logger.info(String.format(message, sameTableConfig.getNode_names(), sameTableConfig.getRelaction_names(),
				DateUtil.d2string(new Date(Long.parseLong(ARGSMAP.get(STARTTIME)))),
				DateUtil.d2string(new Date(Long.parseLong(ARGSMAP.get(ENDTIME))))));

		if (sameTableConfig.getTable_limit() > 0) {
			scan.setFilter(new PageFilter(sameTableConfig.getTable_limit()));
		}

		ResultScanner scanner = table.getScanner(scan);

		List<Result> results = new ArrayList<Result>();

		// 放置node对应ID
		Map<String, Long> mapNodeData = new HashMap<String, Long>();
		// 放置mapRelationship对应ID
		Map<String, Long> mapRelationshipData = new HashMap<String, Long>();

		//boolean operation = ARGSMAP.get(RUNMODE).equals(INCREMENT);

		for (Result result : scanner) {
			results.add(result);

			if (!sameTableConfig.getNodes().isEmpty()) {
				for (NodeConfig nodeConfig : sameTableConfig.getNodes()) {
					LabelConfig labelConfig = configInfo.getLabels().get(nodeConfig.getLabel_name());

					// 设置是否需要查询和保存
					String node_number = LoadConfigUtil.getResultData(result, nodeConfig.getPrimary_property());
					//if (operation) {
					mapNodeData.put(String.format("%s_%s", labelConfig.getCode(), node_number), -1L);
					//}
				}
			}

			if (!sameTableConfig.getRelactions().isEmpty()) {
				for (RelactionConfig relactionConfig : sameTableConfig.getRelactions()) {
					/**
					 * start
					 */
					LabelConfig labelConfig = configInfo.getLabels()
							.get(relactionConfig.getStart_property().getLabel_name());
					// 默认设置为未保存
					String start_node_number = LoadConfigUtil.getResultData(result,
							relactionConfig.getStart_property());

					mapNodeData.put(String.format("%s_%s", labelConfig.getCode(), start_node_number), -1L);
					/**
					 * end
					 */
					labelConfig = configInfo.getLabels().get(relactionConfig.getEnd_property().getLabel_name());
					String end_node_number = LoadConfigUtil.getResultData(result, relactionConfig.getEnd_property());
					mapNodeData.put(String.format("%s_%s", labelConfig.getCode(), end_node_number), -1L);

					//if (operation) {
						String key = String.format("%s_%s_%s_%s", labelConfig.getCode(), start_node_number, end_node_number,
								relactionConfig.getRelship_Type().name());
						int fk = Math.floorMod(key.hashCode(), 20);
						key =  fk >= 10 ? (fk + "_" + (key.hashCode() + start_node_number.hashCode() + end_node_number.hashCode()) ) 
									    : ("0" + fk + "_" + (key.hashCode() + start_node_number.hashCode() + end_node_number.hashCode()));
						mapRelationshipData.put(key, -1L);
					//}
				}

			}
			if ( (mapNodeData.size() > 0 && mapNodeData.size() % configInfo.getNode_index_search_count() == 0) 
					|| (results.size() > 0 && results.size() % configInfo.getNode_index_search_count() == 0) 
					|| (mapRelationshipData.size() > 0 && mapRelationshipData.size() % configInfo.getNode_index_search_count() == 0)) {
				// 保存数据到neo4j
				this.save(sameTableConfig, inserter, results, mapNodeData, mapRelationshipData);
			}
		}

		if (results.size() > 0) {
			// 保存数据到neo4j
			this.save(sameTableConfig, inserter, results, mapNodeData, mapRelationshipData);
		}
		table.close();
		message = "      nodes \t [%s] \t relactions [%s] \t total     \t [%d] \t cost [%d]";
		logger.info(String.format(message, sameTableConfig.getNode_names(), sameTableConfig.getRelaction_names(),
				counts, (System.currentTimeMillis() - start)));
	}

	public void save(SameTableConfig sameTableConfig, BatchInserter inserter, List<Result> results,
			Map<String, Long> mapNodeData, Map<String, Long> mapRelationshipData) {
		String message = "      nodes \t [%s] \t relactions [%s] \t count     \t [%d] \t spend [%d]";
		nodeIdServer.getNeo4jMappingIndex(configInfo.getNode_index_search_table(), mapNodeData, sameTableConfig,"node");
		nodeIdServer.getNeo4jMappingIndex(configInfo.getRelationship_index_search_table(), mapRelationshipData,sameTableConfig,"relaction");
		long start = System.currentTimeMillis();
		long _step_time = start;
		List<String[]> relactionList = new ArrayList<String[]>();
		List<String[]> nodeList = new ArrayList<String[]>();
		String[] tmp = null;
		boolean isRelaction = false;
		boolean isNode = false;
		for (Result result : results) {
			// 先保存node
			if (!sameTableConfig.getNodes().isEmpty()) {
				isNode = true;
				for (NodeConfig nodeConfig : sameTableConfig.getNodes()) {
					tmp = this.saveNode(inserter, result, nodeConfig, mapNodeData);
					if (null != tmp) {
						nodeList.add(tmp);
					}
				}
			}
			// 再保存relaction
			if (!sameTableConfig.getRelactions().isEmpty()) {
				isRelaction = true;
				for (RelactionConfig relactionConfig : sameTableConfig.getRelactions()) {
					tmp = this.saveRelaction(inserter, result, relactionConfig, mapNodeData, mapRelationshipData);
					if (null != tmp) {
						relactionList.add(tmp);
					}
				}
			}
			counts++;
		}

		logger.info(String.format(message, sameTableConfig.getNode_names(), sameTableConfig.getRelaction_names(),
				counts, (System.currentTimeMillis() - _step_time)));
		_step_time = System.currentTimeMillis();
		// index数据落地
		if (isNode) {
			nodeIdServer.saveNeo4jMappingIndex(configInfo.getNode_index_search_table(), nodeList, sameTableConfig,"node");
		}
		if (isRelaction) {
			nodeIdServer.saveNeo4jMappingIndex(configInfo.getRelationship_index_search_table(), relactionList,
					sameTableConfig,"relaction");
		}
		// 数据全部清除
		mapRelationshipData.clear();
		mapNodeData.clear();
		results.clear();
	}

	public String[] saveNode(BatchInserter inserter, Result result, NodeConfig nodeConfig,
			Map<String, Long> mapNodeData) {
		String[] resultArray = null;
		Label label = DynamicLabel.label(nodeConfig.getLabel_name());
		// 获取唯一标识
		LabelConfig labelConfig = configInfo.getLabels().get(nodeConfig.getLabel_name());
		String node_number = LoadConfigUtil.getResultData(result, nodeConfig.getPrimary_property());
		
		//如果是证件号码，证件号码校验
		if ( "zjhm".equalsIgnoreCase(nodeConfig.getPrimary_property().getHbase_dataformat()) ) {
			if ( !IDCardUtil.isIDCard(node_number) ) {
				return null;
			}
		}
		
		Long nodeId = mapNodeData.get(labelConfig.getCode() + "_" + node_number);

		if (null == nodeId || nodeId == -1) {
			// 设置node属性值
			Map<String, Object> properties = new HashMap<String, Object>();
			properties.put(nodeConfig.getPrimary_property().getNeo4j_property(), node_number);
			for (PropertyConfig propertyConfig : nodeConfig.getProperties()) {
				String propertyValue = LoadConfigUtil.getResultData(result, propertyConfig);
				//值过滤,并且过滤值不设置属性
				if ( !validateProperTyValueForFilter(propertyConfig,propertyValue) ) {
					return null;
				}
				if ( StringUtil.isNotBlank(propertyConfig.getNeo4j_property()) ) {	
					properties.put(propertyConfig.getNeo4j_property(),propertyValue);
				}
			}
			nodeId = inserter.createNode(properties, label);
			mapNodeData.put(labelConfig.getCode() + "_" + node_number, nodeId);
			return new String[] { labelConfig.getCode() + "_" + node_number, String.valueOf(nodeId) };
		} else {
			Map<String, Object> properties = inserter.getNodeProperties(nodeId);
			properties.put(nodeConfig.getPrimary_property().getNeo4j_property(), node_number);
			for (PropertyConfig propertyConfig : nodeConfig.getProperties()) {
				String propertyValue = LoadConfigUtil.getResultData(result, propertyConfig);
				//值过滤,并且过滤值不设置属性
				if ( !validateProperTyValueForFilter(propertyConfig,propertyValue) ) {
					return null;
				}
				if ( StringUtil.isNotBlank(propertyConfig.getNeo4j_property()) ) {	
					properties.put(propertyConfig.getNeo4j_property(),propertyValue);
				}
			}
			inserter.setNodeProperties(nodeId, properties);
		}
		return resultArray;
	}

	public String[] saveRelaction(BatchInserter inserter, Result result, RelactionConfig relactionConfig,
			Map<String, Long> mapNodeData, Map<String, Long> mapRelationshipData) {
		String relaction_habse_family = relactionConfig.getHbase_family();
		String relaction_hbase_column = relactionConfig.getHbase_field();
		boolean flag = false;
		if (!StringUtil.isBlank(relaction_habse_family) && !StringUtil.isBlank(relaction_hbase_column)) {
			if (result.containsColumn(Bytes.toBytes(relaction_habse_family), Bytes.toBytes(relaction_hbase_column))) {
				// 需要保存关系数据
				flag = true;
			}
		} else {
			// 需要保存关系数据
			flag = true;
		}

		if (flag) {
			/**
			 * start
			 */
			LabelConfig labelConfig = configInfo.getLabels().get(relactionConfig.getStart_property().getLabel_name());
			String start_node_number = LoadConfigUtil.getResultData(result, relactionConfig.getStart_property());
			Long snode_number = mapNodeData.get(String.format("%s_%s", labelConfig.getCode(), start_node_number));
			if (snode_number == null || snode_number == -1) {
				return null;
			}

			/**
			 * end
			 */
			labelConfig = configInfo.getLabels().get(relactionConfig.getEnd_property().getLabel_name());
			String end_node_number = LoadConfigUtil.getResultData(result, relactionConfig.getEnd_property());
			Long enode_numbe = mapNodeData.get(String.format("%s_%s", labelConfig.getCode(), end_node_number));
			if (enode_numbe == null || enode_numbe == -1) {
				return null;
			}
			String key = String.format("%s_%s_%s_%s", labelConfig.getCode(), start_node_number, end_node_number,
					relactionConfig.getRelship_Type().name());
			int fk = Math.floorMod(key.hashCode(), 20);
			key =  fk >= 10 ? (fk + "_" + (key.hashCode() + start_node_number.hashCode() + end_node_number.hashCode()) ) 
						    : ("0" + fk + "_" + (key.hashCode() + start_node_number.hashCode() + end_node_number.hashCode()));
			Long relationshipId = mapRelationshipData.get(key);
			if (null == relationshipId || relationshipId == -1) {
				/**
				 * 设置relaction属性值
				 */
				Map<String, Object> properties = new HashMap<String, Object>();

				for (PropertyConfig propertyConfig : relactionConfig.getProperties()) {
					/*properties.put(propertyConfig.getNeo4j_property(),
							LoadConfigUtil.getResultData(result, propertyConfig));*/
					String propertyValue = LoadConfigUtil.getResultData(result, propertyConfig);
					//值过滤,并且过滤值不设置属性
					if ( !validateProperTyValueForFilter(propertyConfig,propertyValue) ) {
						return null;
					}
					if ( StringUtil.isNotBlank(propertyConfig.getNeo4j_property()) ) {	
						properties.put(propertyConfig.getNeo4j_property(),propertyValue);
					}
				}
				relationshipId = inserter.createRelationship(snode_number, enode_numbe,
						relactionConfig.getRelship_Type(), properties);
				return new String[] { key, String.valueOf(relationshipId) };
			} else {
				/**
				 * 设置relaction属性值
				 */
				Map<String, Object> properties = inserter.getRelationshipProperties(relationshipId);
				for (PropertyConfig propertyConfig : relactionConfig.getProperties()) {
					/*properties.put(propertyConfig.getNeo4j_property(),
							LoadConfigUtil.getResultData(result, propertyConfig));*/
					String propertyValue = LoadConfigUtil.getResultData(result, propertyConfig);
					//值过滤,并且过滤值不设置属性
					if ( !validateProperTyValueForFilter(propertyConfig,propertyValue) ) {
						return null;
					}
					if ( StringUtil.isNotBlank(propertyConfig.getNeo4j_property()) ) {	
						properties.put(propertyConfig.getNeo4j_property(),propertyValue);
					}
				}
				inserter.setRelationshipProperties(relationshipId, properties);
			}
		}
		return null;
	}
	
	public boolean validateProperTyValueForFilter(PropertyConfig propertyConfig,String propertyValue ) {
		//判断是否需要校验
		if ( "validate".equalsIgnoreCase(propertyConfig.getProperty_type()) ) {
			String[] values = propertyConfig.getHbase_dataformat().split(",");
			for ( String value : values ) {
				//校验到有值，返回校验通过
				if (value.equals(propertyValue)) {
					return true;
				}
			}
			//校验到没值，返回校验不通过
			return false;
		} else {
			//不需要校验，返回通过
			return true;
		}
	}

}
