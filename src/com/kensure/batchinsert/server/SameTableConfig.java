package com.kensure.batchinsert.server;

import java.util.ArrayList;
import java.util.List;

import com.kensure.batchinsert.common.NodeConfig;
import com.kensure.batchinsert.common.RelactionConfig;

public class SameTableConfig {
	
	private String table_name;
	
	private int table_limit = 0;
	
	private List<NodeConfig> nodes = new ArrayList<NodeConfig>();
	
	private List<RelactionConfig> relactions = new ArrayList<RelactionConfig>();
	
	private String node_names = "";
	
	private String relaction_names = "";
	
	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public int getTable_limit() {
		return table_limit;
	}

	public void setTable_limit(int table_limit) {
		if ( this.table_limit <  table_limit) {
			this.table_limit = table_limit;
		}
	}

	public List<NodeConfig> getNodes() {
		return nodes;
	}

	public void addNodes(NodeConfig node) {
		this.nodes.add(node);
		if ( this.node_names.length() > 0 ) {
			this.node_names = this.node_names + ",";
		}
		this.node_names = this.node_names + node.getLabel_name();
	}

	public List<RelactionConfig> getRelactions() {
		return relactions;
	}

	public void addRelactions(RelactionConfig relaction) {
		this.relactions.add(relaction);
		if ( this.relaction_names.length() > 0 ) {
			this.relaction_names = this.relaction_names + ",";
		}
		this.relaction_names = this.relaction_names + relaction.getRelship_name();
	}

	public String getNode_names() {
		return node_names;
	}

	public void setNode_names(String node_names) {
		this.node_names = node_names;
	}

	public String getRelaction_names() {
		return relaction_names;
	}

	public void setRelaction_names(String relaction_names) {
		this.relaction_names = relaction_names;
	}
	
	
	
	
}
