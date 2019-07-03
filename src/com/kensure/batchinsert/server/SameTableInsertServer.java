package com.kensure.batchinsert.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.kensure.batchinsert.common.ImportConfig;
import com.kensure.batchinsert.common.LabelConfig;
import com.kensure.batchinsert.common.NodeConfig;
import com.kensure.batchinsert.common.PropertyConfig;
import com.kensure.batchinsert.common.RelactionConfig;
import com.kensure.batchinsert.node.Neo4jNodeData2HbaseServer;
import com.kensure.batchinsert.node.Neo4jNodeDataInfo;
import com.kensure.batchinsert.util.LoadConfigUtil;

public class SameTableInsertServer {
	
	private static final Log logger = LogFactory.getLog(SameTableInsertServer.class);
	
	private Neo4jNodeData2HbaseServer nodeIdServer;
	
	private ImportConfig configInfo;
	
	private int counts = 0;
	
	public SameTableInsertServer(ImportConfig configInfo,Neo4jNodeData2HbaseServer nodeIdServer) {
		this.configInfo = configInfo;
		this.nodeIdServer = nodeIdServer;
	}
	
	public void insert(BatchInserter inserter,SameTableConfig sameTableConfig) throws IOException {
		String message = "nodes \t [%s] \t relactions [%s] \t 开始插入数据 ";
        logger.info( String.format(message, sameTableConfig.getNode_names(),sameTableConfig.getRelaction_names()) );
		counts = 0;
		long start = System.currentTimeMillis();
		Connection conn = nodeIdServer.getConnection();
		
        Table table = conn.getTable(TableName.valueOf(sameTableConfig.getTable_name()));

        Scan scan = new Scan();
        
        if ( sameTableConfig.getTable_limit() > 0 ) {
        	scan.setFilter(new PageFilter(sameTableConfig.getTable_limit()));
        }
        
        ResultScanner scanner = table.getScanner(scan);

        List<Result> results = new ArrayList<Result>();
        
        //放置node对应ID
        ConcurrentHashMap<String,Neo4jNodeDataInfo> mapNodeData = new ConcurrentHashMap<String,Neo4jNodeDataInfo>();
        for (Result result : scanner) {
            results.add(result);
            
            for ( NodeConfig nodeConfig : sameTableConfig.getNodes() ) {
            	LabelConfig labelConfig = configInfo.getLabels().get(nodeConfig.getLabel_name());
            	
            	//设置是否需要查询和保存
            	String node_number = LoadConfigUtil.getResultData(result,nodeConfig.getPrimary_property());
            	//是否需要查询
            	boolean node_get = nodeConfig.isNode_get();
            	//默认数据查询标识
            	boolean isgeted = !node_get;
            	
            	//是否需要保存
            	boolean node_save = nodeConfig.isNode_save();
            	//默认数据保存标识
            	boolean issaved = !node_save;
            	
            	Neo4jNodeDataInfo neo4jNodeDataInfo = new Neo4jNodeDataInfo( labelConfig.getCode() ,node_number,isgeted,issaved );
            	
            	mapNodeData.put(neo4jNodeDataInfo.getLabelRowkey(), neo4jNodeDataInfo);
            }
            
            for ( RelactionConfig relactionConfig : sameTableConfig.getRelactions() ) {
            	
            	//是否需要查询
            	boolean node_get = relactionConfig.isNode_get();
            	//默认数据查询标识
            	boolean isgeted = !node_get;
            	
            	//是否需要保存
            	boolean node_save = relactionConfig.isNode_save();
            	//默认数据保存标识
            	boolean issaved = !node_save;
            	
            	/**
            	 * start
            	 */
            	LabelConfig labelConfig = configInfo.getLabels().get(relactionConfig.getStart_property().getLabel_name());
            	//默认设置为未保存
            	String node_number = LoadConfigUtil.getResultData(result,relactionConfig.getStart_property());
            	Neo4jNodeDataInfo neo4jNodeDataInfo = new Neo4jNodeDataInfo( labelConfig.getCode() ,node_number,isgeted,issaved );
            	
            	mapNodeData.put(neo4jNodeDataInfo.getLabelRowkey(), neo4jNodeDataInfo);
            	
            	/**
            	 * end
            	 */
            	labelConfig = configInfo.getLabels().get(relactionConfig.getEnd_property().getLabel_name());
            	//默认设置为未保存
            	node_number = LoadConfigUtil.getResultData(result,relactionConfig.getEnd_property());
            	neo4jNodeDataInfo = new Neo4jNodeDataInfo( labelConfig.getCode() ,node_number,isgeted,issaved );
            	
            	mapNodeData.put(neo4jNodeDataInfo.getLabelRowkey(), neo4jNodeDataInfo);
            }

            if ( mapNodeData.size() > 0 &&  mapNodeData.size() % configInfo.getNode_index_search_count() == 0) {
            	//保存数据到neo4j
            	this.save(sameTableConfig,inserter, results, mapNodeData); 
            }
        }
        
        if ( results.size() > 0 ) {
        	//保存数据到neo4j
        	this.save(sameTableConfig,inserter, results, mapNodeData);
        }
        table.close();
        message = "nodes \t [%s] \t relactions [%s] \t total     \t [%d] \t cost [%d]";
        logger.info( String.format(message, sameTableConfig.getNode_names(),sameTableConfig.getRelaction_names(),counts,(System.currentTimeMillis() - start)) );
	}
	
	public void save(SameTableConfig sameTableConfig,BatchInserter inserter, List<Result> results, ConcurrentHashMap<String,Neo4jNodeDataInfo> mapNodeData) {
		String message = "nodes \t [%s] \t relactions [%s] \t count     \t [%d] \t spend [%d]";
		//先查询数据
    	mapNodeData = nodeIdServer.getNeo4jLabelIndex(mapNodeData,sameTableConfig);
        long start = System.currentTimeMillis();
        long _step_time = start;
    	for (Result result : results) {
    		//先保存node
	    	for ( NodeConfig nodeConfig : sameTableConfig.getNodes() ) {
	    		this.saveNode(inserter, result, nodeConfig, mapNodeData);
	    	}
	    	//再保存relaction
	    	for ( RelactionConfig relactionConfig : sameTableConfig.getRelactions() ) {
	    		this.saveRelaction(inserter, result, relactionConfig, mapNodeData);
	    	}
	    	
	    	counts++;
	        /*if (counts % 100000 == 0) {
	        	//logger.info("<" + counts + "> \t spend [" + (System.currentTimeMillis() - _step_time) + "]" );
	        	logger.info( String.format(message, sameTableConfig.getNode_names(),sameTableConfig.getRelaction_names(),counts,(System.currentTimeMillis() - _step_time)) );
	            _step_time = System.currentTimeMillis();
	        }*/
    	}
    	
    	logger.info( String.format(message, sameTableConfig.getNode_names(),sameTableConfig.getRelaction_names(),counts,(System.currentTimeMillis() - _step_time)) );
    	_step_time = System.currentTimeMillis();
    	
    	//index数据落地
    	nodeIdServer.saveNeo4jLabelIndex(mapNodeData,sameTableConfig);
    	//数据全部清除
        mapNodeData.clear();
        results.clear();
	}
	
	
	public void saveNode(BatchInserter inserter, Result result, NodeConfig nodeConfig,ConcurrentHashMap<String,Neo4jNodeDataInfo> mapNodeData) {
		Label label = DynamicLabel.label( nodeConfig.getLabel_name() );
		
		//获取唯一标识
		LabelConfig labelConfig = configInfo.getLabels().get(nodeConfig.getLabel_name());
    	String node_number = LoadConfigUtil.getResultData(result,nodeConfig.getPrimary_property());
		
    	Neo4jNodeDataInfo  neo4jNodeDataInfo =  mapNodeData.get(labelConfig.getCode() + "_" + node_number);
    	
    	if ( neo4jNodeDataInfo.getLabelIndex() == null || neo4jNodeDataInfo.getLabelIndex().length() <= 0  ) {
    		//设置node属性值
    		Map<String, Object> properties = new HashMap<String, Object>();
    		
    		properties.put(nodeConfig.getPrimary_property().getNeo4j_property(), node_number);
    		for ( PropertyConfig propertyConfig : nodeConfig.getProperties() ) {
    			properties.put(propertyConfig.getNeo4j_property(), LoadConfigUtil.getResultData(result,propertyConfig));
    		}
    		
    		//插入到neo4j
    		long id = inserter.createNode(properties, label);
    		
    		//更新缓存ID值
    		neo4jNodeDataInfo.setLabelIndex(id+"");
    	} 
	}
	
	
	public Long saveRelaction(BatchInserter inserter, Result result, RelactionConfig relactionConfig,ConcurrentHashMap<String,Neo4jNodeDataInfo> mapNodeData) {
		String relaction_habse_family = relactionConfig.getHbase_family();
		String relaction_hbase_column = relactionConfig.getHbase_field();
		boolean flag = false;
		if ( !StringUtil.isBlank( relaction_habse_family )  &&  !StringUtil.isBlank( relaction_hbase_column ) ) {
			if ( result.containsColumn(Bytes.toBytes( relaction_habse_family ), Bytes.toBytes( relaction_hbase_column )) ) {
				//需要保存关系数据
				flag = true;
			}
		} else {
			//需要保存关系数据
			flag = true;
		}
		
		if (flag) {
		
			/**
	    	 * start
	    	 */
	    	LabelConfig labelConfig = configInfo.getLabels().get(relactionConfig.getStart_property().getLabel_name());
	    	String node_number = LoadConfigUtil.getResultData(result,relactionConfig.getStart_property());
	    	
	    	Long id1 = LoadConfigUtil.getNodeId(labelConfig.getCode(), node_number, mapNodeData);
	    	if ( id1 == null ) {
	    		return null;
	    	}
	    	
	    	/**
	    	 * end
	    	 */
	    	labelConfig = configInfo.getLabels().get(relactionConfig.getEnd_property().getLabel_name());
	    	node_number = LoadConfigUtil.getResultData(result,relactionConfig.getEnd_property());
	    	Long id2 = LoadConfigUtil.getNodeId(labelConfig.getCode(), node_number, mapNodeData);
	    	if ( id2 == null ) {
	    		return null;
	    	}
	    	
	    	/**
	    	 * 设置relaction属性值
	    	 */
			Map<String, Object> properties = new HashMap<String, Object>();
			
			for ( PropertyConfig propertyConfig : relactionConfig.getProperties() ) {
				properties.put(propertyConfig.getNeo4j_property(), LoadConfigUtil.getResultData(result,propertyConfig));
			}
			
			return inserter.createRelationship(id1, id2, relactionConfig.getRelship_Type(), properties);
		} 
		return null;
	}
	

}
