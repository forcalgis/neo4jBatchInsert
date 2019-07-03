package com.kensure.batchinsert.common;

public class PropertyConfig {
	
    private String hbase_family; 
	
	private String hbase_field; 
	
	private String hbase_datatype;
	
    private String hbase_dataformat;
    
    private String neo4j_property;
    
    private String property_type;
    
    private String label_name;

	/**
	 * @return the hbase_family
	 */
	public String getHbase_family() {
		return hbase_family;
	}

	/**
	 * @param hbase_family the hbase_family to set
	 */
	public void setHbase_family(String hbase_family) {
		this.hbase_family = hbase_family;
	}

	/**
	 * @return the hbase_field
	 */
	public String getHbase_field() {
		return hbase_field;
	}

	/**
	 * @param hbase_field the hbase_field to set
	 */
	public void setHbase_field(String hbase_field) {
		this.hbase_field = hbase_field;
	}

	/**
	 * @return the hbase_datatype
	 */
	public String getHbase_datatype() {
		return hbase_datatype;
	}

	/**
	 * @param hbase_datatype the hbase_datatype to set
	 */
	public void setHbase_datatype(String hbase_datatype) {
		this.hbase_datatype = hbase_datatype;
	}

	/**
	 * @return the hbase_dataformat
	 */
	public String getHbase_dataformat() {
		return hbase_dataformat;
	}

	/**
	 * @param hbase_dataformat the hbase_dataformat to set
	 */
	public void setHbase_dataformat(String hbase_dataformat) {
		this.hbase_dataformat = hbase_dataformat;
	}

	/**
	 * @return the neo4j_property
	 */
	public String getNeo4j_property() {
		return neo4j_property;
	}

	/**
	 * @param neo4j_property the neo4j_property to set
	 */
	public void setNeo4j_property(String neo4j_property) {
		this.neo4j_property = neo4j_property;
	}

	public String getProperty_type() {
		return property_type;
	}

	public void setProperty_type(String property_type) {
		this.property_type = property_type;
	}

	public String getLabel_name() {
		return label_name;
	}

	public void setLabel_name(String label_name) {
		this.label_name = label_name;
	} 

	
}
