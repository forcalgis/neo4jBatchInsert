package com.kensure.batchinsert.node;

public class Neo4jNodeDataInfo {
	//类型标识
	private String labelCode;
	//类型主键
	private String labelNumber;
	//hbase对应rowkey
	private String labelRowkey;
	//
	private String labelIndex;
	
	//保存标识 默认为false
	private boolean saved = false;
	
	//保存标识 默认为false
	private boolean geted = false;
	
	public Neo4jNodeDataInfo() {
		
	}
	


	public Neo4jNodeDataInfo(String labelCode, String labelNumber, boolean geted, boolean saved) {
		super();
		this.labelCode = labelCode;
		this.labelNumber = labelNumber;
		this.labelRowkey = labelCode + "_" + labelNumber;
		this.geted = geted;
		this.saved = saved;
	}
	
	public String getLabelCode() {
		return labelCode;
	}
	
	public void setLabelCode(String labelCode) {
		this.labelCode = labelCode;
	}
	
	public String getLabelNumber() {
		return labelNumber;
	}
	
	public void setLabelNumber(String labelNumber) {
		this.labelNumber = labelNumber;
	}
	
	public String getLabelRowkey() {
		return labelRowkey;
	}
	
	public void setLabelRowkey(String labelRowkey) {
		this.labelRowkey = labelRowkey;
	}
	
	public boolean isSaved() {
		return saved;
	}
	
	public void setSaved(boolean saved) {
		this.saved = saved;
	}



	public String getLabelIndex() {
		return labelIndex;
	}



	public void setLabelIndex(String labelIndex) {
		this.labelIndex = labelIndex;
	}



	/**
	 * @return the geted
	 */
	public boolean isGeted() {
		return geted;
	}



	/**
	 * @param geted the geted to set
	 */
	public void setGeted(boolean geted) {
		this.geted = geted;
	}
	
}
