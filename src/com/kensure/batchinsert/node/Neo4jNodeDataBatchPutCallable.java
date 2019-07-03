package com.kensure.batchinsert.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Neo4jNodeDataBatchPutCallable implements
		Callable<Integer> {
	private List<Neo4jNodeDataInfo> datas;
	private Neo4jNodeDataConfig labelIndexConfig;
	private Connection conn;

	public Neo4jNodeDataBatchPutCallable(Connection conn,
			Neo4jNodeDataConfig labelIndexConfig, List<Neo4jNodeDataInfo> lstKeys) {
		this.conn = conn;
		this.labelIndexConfig = labelIndexConfig;
		this.datas = lstKeys;
	}

	public Integer call() throws Exception {
		Integer savedCount = 0;
		Table table = conn
				.getTable(TableName.valueOf(labelIndexConfig.getNodeTable()));
		try {
			int TOTAL = datas.size();
			List<Put> list = new ArrayList<Put>();
			int index = 0;
			while (  index < TOTAL ){
				Neo4jNodeDataInfo data = datas.get(index);
				
				if ( !data.isSaved() ) {
					savedCount ++;
					Put p1 = new Put(Bytes.toBytes( data.getLabelRowkey()  ));  
			        p1.addColumn(Bytes.toBytes(labelIndexConfig.getFamily()), Bytes.toBytes(labelIndexConfig.getColumn()), Bytes.toBytes(data.getLabelIndex()));  
				    list.add(p1);
				}
			    
			    if ( list.size() >= labelIndexConfig.getSaveBatchSize() ) {
			    	table.put(list);
			    	list.clear();
			    }
			    
		        index ++;
			}
			
			if ( list.size() > 0 ) {
				table.put(list);
			}
		} catch (IOException e) {
			System.out.println(e);
			throw new IOException("save data errors", e);
		} finally {
			table.close();
		}

		return savedCount;
	}
}
