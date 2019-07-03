package com.kensure.batchinsert.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Neo4jMappingDataBatchPutCallable implements Callable<Integer> {
	private List<String[]> datas;
	private Neo4jNodeDataConfig labelIndexConfig;
	private Connection conn;
	private String tname;

	public Neo4jMappingDataBatchPutCallable(Connection conn, String tname, Neo4jNodeDataConfig labelIndexConfig,
			List<String[]> lstKeys) {
		this.conn = conn;
		this.labelIndexConfig = labelIndexConfig;
		this.datas = lstKeys;
		this.tname = tname;
	}

	public Integer call() throws Exception {
		Integer savedCount = 0;
		Table table = conn.getTable(TableName.valueOf(this.tname));
		try {
			int TOTAL = datas.size();
			List<Put> list = new ArrayList<Put>();
			int index = 0;
			while (index < TOTAL) {
				String[] data = datas.get(index);
				savedCount++;
				Put p1 = new Put(Bytes.toBytes(data[0]));
				p1.addColumn(Bytes.toBytes(labelIndexConfig.getFamily()), Bytes.toBytes(labelIndexConfig.getColumn()),
						Bytes.toBytes(data[1]));
				list.add(p1);

				if (list.size() >= labelIndexConfig.getSaveBatchSize()) {
					table.put(list);
					list.clear();
				}

				index++;
			}

			if (list.size() > 0) {
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
