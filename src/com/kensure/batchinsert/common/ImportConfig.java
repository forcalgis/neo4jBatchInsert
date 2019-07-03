package com.kensure.batchinsert.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ImportConfig {
	// hbase zookeeper 端口 hbase.zookeeper.property.clientPort
	private String zookeeper_clientPort;
	// hbase zookeeper 集群地址 zookeeper.quorum
	private String zookeeper_quorum;
	// neo4j 数据库路径 neo4j.database.path
	private String neo4j_database_path;

	// neo4j 数据库路径 neo4j.bin.path
	private String neo4j_bin_path;

	// neo4j 数据库路径 neo4j.conf.path
	private String neo4j_conf_path;

	// neo4j 数据库导入之前是否删除数据 neo4j.database.beforeexc.deleted
	private String neo4j_database_beforeexc_deleted;
	// 批量查询节点ID数量阀值，默认20W
	private int node_index_search_count;
	// 批量查询节点ID多线数，默认10
	private int node_index_search_parallel;
	// 节点ID存储数据前是否删除老数据
	private String node_index_search_beforeexc_deleted;
	// ID存储数据的表名
	private String node_index_search_table;
	// ID存储数据的表名
	private String relationship_index_search_table;
	// ID存储数据的列簇
	private String node_index_search_family;
	// ID存储数据的列名
	private String node_index_search_column;
	// ID存储数据的表预分区
	private String node_index_search_table_partions;
	// 执行顺序
	private String neo4j_batch_insert_table_order;
	// 导入节点配置
	private List<NodeConfig> nodes = new ArrayList<NodeConfig>();
	// 导入关系配置
	private List<RelactionConfig> relactions = new ArrayList<RelactionConfig>();
	// 导入节点类型配置
	private ConcurrentHashMap<String, LabelConfig> labels = new ConcurrentHashMap<String, LabelConfig>();

	public String getZookeeper_clientPort() {
		return zookeeper_clientPort;
	}

	public void setZookeeper_clientPort(String zookeeper_clientPort) {
		this.zookeeper_clientPort = zookeeper_clientPort;
	}

	public String getZookeeper_quorum() {
		return zookeeper_quorum;
	}

	public void setZookeeper_quorum(String zookeeper_quorum) {
		this.zookeeper_quorum = zookeeper_quorum;
	}

	public String getNeo4j_database_path() {
		return neo4j_database_path;
	}

	public void setNeo4j_database_path(String neo4j_database_path) {
		this.neo4j_database_path = neo4j_database_path;
	}

	public String getNeo4j_bin_path() {
		return neo4j_bin_path;
	}

	public void setNeo4j_bin_path(String neo4j_bin_path) {
		this.neo4j_bin_path = neo4j_bin_path;
	}

	public String getNeo4j_conf_path() {
		return neo4j_conf_path;
	}

	public void setNeo4j_conf_path(String neo4j_conf_path) {
		this.neo4j_conf_path = neo4j_conf_path;
	}

	public String getNeo4j_database_beforeexc_deleted() {
		return neo4j_database_beforeexc_deleted;
	}

	public void setNeo4j_database_beforeexc_deleted(String neo4j_database_beforeexc_deleted) {
		this.neo4j_database_beforeexc_deleted = neo4j_database_beforeexc_deleted;
	}

	public int getNode_index_search_count() {
		return node_index_search_count;
	}

	public void setNode_index_search_count(int node_index_search_count) {
		this.node_index_search_count = node_index_search_count;
	}

	public int getNode_index_search_parallel() {
		return node_index_search_parallel;
	}

	public void setNode_index_search_parallel(int node_index_search_parallel) {
		this.node_index_search_parallel = node_index_search_parallel;
	}

	public String getNode_index_search_beforeexc_deleted() {
		return node_index_search_beforeexc_deleted;
	}

	public void setNode_index_search_beforeexc_deleted(String node_index_search_beforeexc_deleted) {
		this.node_index_search_beforeexc_deleted = node_index_search_beforeexc_deleted;
	}

	public String getNode_index_search_table() {
		return node_index_search_table;
	}

	public void setNode_index_search_table(String node_index_search_table) {
		this.node_index_search_table = node_index_search_table;
	}

	public String getRelationship_index_search_table() {
		return relationship_index_search_table;
	}

	public void setRelationship_index_search_table(String relationship_index_search_table) {
		this.relationship_index_search_table = relationship_index_search_table;
	}

	public String getNode_index_search_family() {
		return node_index_search_family;
	}

	public void setNode_index_search_family(String node_index_search_family) {
		this.node_index_search_family = node_index_search_family;
	}

	public String getNode_index_search_column() {
		return node_index_search_column;
	}

	public void setNode_index_search_column(String node_index_search_column) {
		this.node_index_search_column = node_index_search_column;
	}

	public String getNode_index_search_table_partions() {
		return node_index_search_table_partions;
	}

	public void setNode_index_search_table_partions(String node_index_search_table_partions) {
		this.node_index_search_table_partions = node_index_search_table_partions;
	}

	public List<NodeConfig> getNodes() {
		return nodes;
	}

	public void setNodes(List<NodeConfig> nodes) {
		this.nodes = nodes;
	}

	public List<RelactionConfig> getRelactions() {
		return relactions;
	}

	public void setRelactions(List<RelactionConfig> relactions) {
		this.relactions = relactions;
	}

	public ConcurrentHashMap<String, LabelConfig> getLabels() {
		return labels;
	}

	public void setLabels(ConcurrentHashMap<String, LabelConfig> labels) {
		this.labels = labels;
	}

	public String getNeo4j_batch_insert_table_order() {
		return neo4j_batch_insert_table_order;
	}

	public void setNeo4j_batch_insert_table_order(String neo4j_batch_insert_table_order) {
		this.neo4j_batch_insert_table_order = neo4j_batch_insert_table_order;
	}
}
