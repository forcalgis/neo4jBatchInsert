package com.kensure.batchinsert.common;

import java.util.ArrayList;
import java.util.List;

public class NodeConfig {
	
	private String label_name;
	
	private String table_name;
	
	private String table_limit;
	
    private boolean node_save = true;
    
    private boolean node_get = true;
	
	private List<PropertyConfig> properties = new ArrayList<PropertyConfig>();
	
	private PropertyConfig primary_property;
	
	
	public NodeConfig(String label_name, String table_name,String table_limit,String node_get ,String node_save) {
		super();
		this.label_name = label_name;
		this.table_name = table_name;
		this.table_limit = table_limit;
		this.node_get = "false".equalsIgnoreCase(node_get) ? false : true;
		this.node_save = "false".equalsIgnoreCase(node_save) ? false : true;
	}

	public String getLabel_name() {
		return label_name;
	}


	public void setLabel_name(String label_name) {
		this.label_name = label_name;
	}


	public List<PropertyConfig> getProperties() {
		return properties;
	}


	public void setProperties(List<PropertyConfig> properties) {
		this.properties = properties;
	}


	public PropertyConfig getPrimary_property() {
		return primary_property;
	}


	public void setPrimary_property(PropertyConfig primary_property) {
		this.primary_property = primary_property;
	}


	public String getTable_name() {
		return table_name;
	}


	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public String getTable_limit() {
		return table_limit;
	}

	public void setTable_limit(String table_limit) {
		this.table_limit = table_limit;
	}

	/**
	 * @return the node_save
	 */
	public boolean isNode_save() {
		return node_save;
	}

	/**
	 * @param node_save the node_save to set
	 */
	public void setNode_save(boolean node_save) {
		this.node_save = node_save;
	}

	/**
	 * @return the node_get
	 */
	public boolean isNode_get() {
		return node_get;
	}

	/**
	 * @param node_get the node_get to set
	 */
	public void setNode_get(boolean node_get) {
		this.node_get = node_get;
	}


}
