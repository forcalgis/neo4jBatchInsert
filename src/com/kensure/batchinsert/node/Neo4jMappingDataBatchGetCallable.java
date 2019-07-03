package com.kensure.batchinsert.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Neo4jMappingDataBatchGetCallable implements Callable<Map<String, String>> {
	private Set<String> keys;
	private Neo4jNodeDataConfig labelIndexConfig;
	private Connection conn;
	private String tname;

	public Neo4jMappingDataBatchGetCallable(Connection conn, String tname, Neo4jNodeDataConfig labelIndexConfig,
			Set<String> lstKeys) {
		this.conn = conn;
		this.tname = tname;
		this.labelIndexConfig = labelIndexConfig;
		this.keys = lstKeys;
	}

	public Map<String, String> call() throws Exception {
		Map<String, String> hashRet = null;
		Table table = conn.getTable(TableName.valueOf(tname));
		try {
			List<Get> lstGet = new ArrayList<Get>();
			for (String key : keys) {
				Get g = new Get(Bytes.toBytes(key));
				g.addColumn(Bytes.toBytes(labelIndexConfig.getFamily()), Bytes.toBytes(labelIndexConfig.getColumn()));
				lstGet.add(g);
			}
			Result[] res = null;
			res = table.get(lstGet);

			if (res != null && res.length > 0) {
				hashRet = new ConcurrentHashMap<String, String>(res.length);
				for (Result re : res) {
					if (re != null && !re.isEmpty()) {
						String rowkey = Bytes.toString(re.getRow());
						String labelIndex = Bytes.toString(re.getValue(Bytes.toBytes(labelIndexConfig.getFamily()),
								Bytes.toBytes(labelIndexConfig.getColumn())));
						hashRet.put(rowkey, labelIndex);
					}
				}
			}
		} catch (IOException e) {
			System.out.println(e);
			throw new IOException("get data errors", e);
		} finally {
			table.close();
		}
		return hashRet;
	}
}
