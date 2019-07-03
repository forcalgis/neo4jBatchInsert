package com.kensure.batchinsert.node;

import com.kensure.batchinsert.common.ImportConfig;

public class Neo4jNodeDataConfig {
	
	
	
	private String nodeTable;

	private String relationshipTable;
	
	private String family = "base";
	
	private String column = "labelindex";
	
	private int parallel = 5;
	
	private String partions = "00,01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19";
	
	private int saveBatchSize = 5000;
	
	public Neo4jNodeDataConfig() {
		
	}
	
	public Neo4jNodeDataConfig(ImportConfig configInfo) {
		this(configInfo.getNode_index_search_table(), configInfo.getRelationship_index_search_table(),
				configInfo.getNode_index_search_family(), configInfo.getNode_index_search_column(),
				configInfo.getNode_index_search_table_partions(), configInfo.getNode_index_search_parallel());
	}
	
	public Neo4jNodeDataConfig(String nodeTable, String relationshipTable, String family, String column,
			int parallel) {
		super();
		this.nodeTable = nodeTable;
		this.relationshipTable = relationshipTable;
		this.family = family;
		this.column = column;
		this.parallel = parallel;
	}
	
	public Neo4jNodeDataConfig(String nodeTable, String relationshipTable, String family, String column,String partions,
			int parallel) {
		super();
		this.nodeTable = nodeTable;
		this.relationshipTable = relationshipTable;
		this.family = family;
		this.column = column;
		this.parallel = parallel;
		this.partions = partions;
	}


	public String getFamily() {
		return family;
	}

	public void setFamily(String family) {
		this.family = family;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public int getParallel() {
		return parallel;
	}

	public void setParallel(int parallel) {
		this.parallel = parallel;
	}
	
	public String getNodeTable() {
		return nodeTable;
	}

	public void setNodeTable(String nodeTable) {
		this.nodeTable = nodeTable;
	}

	public String getRelationshipTable() {
		return relationshipTable;
	}

	public void setRelationshipTable(String relationshipTable) {
		this.relationshipTable = relationshipTable;
	}

	public String getPartions() {
		return partions;
	}

	public void setPartions(String partions) {
		this.partions = partions;
	}

	public int getSaveBatchSize() {
		return saveBatchSize;
	}


	public void setSaveBatchSize(int saveBatchSize) {
		this.saveBatchSize = saveBatchSize;
	}
	
	

}
