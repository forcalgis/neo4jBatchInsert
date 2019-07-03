package com.kensure.batchinsert.common;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;

public class RelactionConfig {
	
	private String hbase_family;
	
	private String hbase_field;
	
	private String table_name;
	
	private String table_limit;
	
	private PropertyConfig start_property;
	
	private PropertyConfig end_property;
	
	private String relship_name;
	
	private String relship_displayname;
	
	private RelationshipType relship_Type;
	
	private boolean node_save = true;
	    
	private boolean node_get = true;
	
	private List<PropertyConfig> properties = new ArrayList<PropertyConfig>();

	public RelactionConfig(String hbase_family, String hbase_field,
			String table_name, String relship_name, String relship_displayname,String node_get ,String node_save) {
		super();
		this.hbase_family = hbase_family;
		this.hbase_field = hbase_field;
		this.table_name = table_name;
		this.relship_name = relship_name;
		this.relship_displayname = relship_displayname;
		this.relship_Type = DynamicRelationshipType.withName( relship_name ); 
		this.node_get = "false".equalsIgnoreCase(node_get) ? false : true;
		this.node_save = "false".equalsIgnoreCase(node_save) ? false : true;
	}

	public String getHbase_family() {
		return hbase_family;
	}

	public void setHbase_family(String hbase_family) {
		this.hbase_family = hbase_family;
	}

	public String getHbase_field() {
		return hbase_field;
	}

	public void setHbase_field(String hbase_field) {
		this.hbase_field = hbase_field;
	}

	public String getRelship_name() {
		return relship_name;
	}

	public void setRelship_name(String relship_name) {
		this.relship_name = relship_name;
		this.relship_Type = DynamicRelationshipType.withName( relship_name ); 
	}

	public String getRelship_displayname() {
		return relship_displayname;
	}

	public void setRelship_displayname(String relship_displayname) {
		this.relship_displayname = relship_displayname;
	}

	public RelationshipType getRelship_Type() {
		return relship_Type;
	}

	public void setRelship_Type(RelationshipType relship_Type) {
		this.relship_Type = relship_Type;
	}

	public List<PropertyConfig> getProperties() {
		return properties;
	}

	public void setProperties(List<PropertyConfig> properties) {
		this.properties = properties;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public PropertyConfig getStart_property() {
		return start_property;
	}

	public void setStart_property(PropertyConfig start_property) {
		this.start_property = start_property;
	}

	public PropertyConfig getEnd_property() {
		return end_property;
	}

	public void setEnd_property(PropertyConfig end_property) {
		this.end_property = end_property;
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
