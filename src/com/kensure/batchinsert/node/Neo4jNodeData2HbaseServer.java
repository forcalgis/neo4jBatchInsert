package com.kensure.batchinsert.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kensure.batchinsert.server.SameTableConfig;

public class Neo4jNodeData2HbaseServer {

	private static final Log logger = LogFactory.getLog(Neo4jNodeData2HbaseServer.class);

	private Neo4jNodeDataConfig labelIndexConfig;

	// private SameTableConfig sameTableConfig;

	private Connection conn;

	public Neo4jNodeData2HbaseServer(Connection conn, Neo4jNodeDataConfig labelIndexConfig) throws IOException {
		this.conn = conn;
		this.labelIndexConfig = labelIndexConfig;
	}

	public Connection getConnection() throws IOException {
		return conn;
	}

	/**
	 * 查询neo4j node对应的ID值
	 * 
	 * @param mapNodeData
	 *            key:Neo4jNodeDataInfo.labelRowkey
	 * @return
	 */
	public ConcurrentHashMap<String, Neo4jNodeDataInfo> getNeo4jLabelIndex(
			ConcurrentHashMap<String, Neo4jNodeDataInfo> mapNodeData, SameTableConfig sameTableConfig) {

		/*
		 * List<String> lstkeys = new ArrayList<String>(); for ( Neo4jNodeDataInfo
		 * nodeDataInfo : mapNodeData.values() ) {
		 * lstkeys.add(nodeDataInfo.getLabelRowkey()); }
		 * 
		 * ConcurrentHashMap<String, String> searchMap =
		 * this.getNeo4jLabelIndex(lstkeys);
		 * 
		 * //循环查询到到key for ( String rowkey : searchMap.keySet()) { //第二位 String number =
		 * rowkey.split("_")[1];
		 * 
		 * Neo4jNodeDataInfo nodeDataInfo = mapNodeData.get(number);
		 * nodeDataInfo.setLabelIndex(searchMap.get(rowkey));
		 * nodeDataInfo.setSaved(true);
		 * 
		 * }
		 */
		long start = System.currentTimeMillis();
		List<String> lstkeys = new ArrayList<String>();

		for (String key : mapNodeData.keySet()) {
			Neo4jNodeDataInfo dataInfo = mapNodeData.get(key);
			if (!dataInfo.isGeted()) {
				lstkeys.add(key);
			}
		}

		if (lstkeys.size() == 0) {
			String message = "nodes \t [%s] \t relactions [%s] \t node get \t [%d] \t spend [%d]";
			logger.info(String.format(message, sameTableConfig.getNode_names(), sameTableConfig.getRelaction_names(), 0,
					(System.currentTimeMillis() - start)));
			return mapNodeData;
		}

		ConcurrentHashMap<String, String> searchMap = this.getNeo4jLabelIndex(lstkeys);

		// 循环查询到到key
		for (String rowkey : searchMap.keySet()) {
			// 设值
			Neo4jNodeDataInfo nodeDataInfo = mapNodeData.get(rowkey);
			nodeDataInfo.setLabelIndex(searchMap.get(rowkey));
			nodeDataInfo.setSaved(true);
		}

		int search_count = searchMap.size();
		String message = "nodes \t [%s] \t relactions [%s] \t node get \t [%d] \t spend [%d]";
		logger.info(String.format(message, sameTableConfig.getNode_names(), sameTableConfig.getRelaction_names(),
				search_count, (System.currentTimeMillis() - start)));

		return mapNodeData;
	}

	public ConcurrentHashMap<String, String> getNeo4jLabelIndex(List<String> lstKeys) {
		ConcurrentHashMap<String, String> hashRet = new ConcurrentHashMap<String, String>();
		int parallel = labelIndexConfig.getParallel();
		List<List<String>> lstBatchKeys = null;
		if (lstKeys.size() < parallel) {
			lstBatchKeys = new ArrayList<List<String>>(1);
			lstBatchKeys.add(lstKeys);
		} else {
			lstBatchKeys = new ArrayList<List<String>>(parallel);
			for (int i = 0; i < parallel; i++) {
				List<String> lst = new ArrayList<String>();
				lstBatchKeys.add(lst);
			}

			for (int i = 0; i < lstKeys.size(); i++) {
				lstBatchKeys.get(i % parallel).add(lstKeys.get(i));
			}
		}

		List<Future<ConcurrentHashMap<String, String>>> futures = new ArrayList<Future<ConcurrentHashMap<String, String>>>();

		ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
		builder.setNameFormat("ParallelBatchQuery");
		ThreadFactory factory = builder.build();
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(lstBatchKeys.size(), factory);

		for (List<String> keys : lstBatchKeys) {
			Callable<ConcurrentHashMap<String, String>> callable = new Neo4jNodeDataBatchGetCallable(conn,
					labelIndexConfig, keys);
			FutureTask<ConcurrentHashMap<String, String>> future = (FutureTask<ConcurrentHashMap<String, String>>) executor
					.submit(callable);
			futures.add(future);
		}
		executor.shutdown();

		// Wait for all the tasks to finish
		try {
			boolean stillRunning = !executor.awaitTermination(300000, TimeUnit.MILLISECONDS);
			if (stillRunning) {
				try {
					executor.shutdownNow();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (InterruptedException e) {
			try {
				Thread.currentThread().interrupt();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}

		// Look for any exception
		for (Future<ConcurrentHashMap<String, String>> f : futures) {
			try {
				if (f != null && f.get() != null) {
					hashRet.putAll(f.get());
				}
			} catch (InterruptedException e) {
				try {
					Thread.currentThread().interrupt();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

		return hashRet;
	}

	public void saveNeo4jMappingIndex(String tname, List<String[]> lstKeys, SameTableConfig sameTableConfig,
			String type) {
		/*
		 * //如果当前只插入关系数据，不保存node数据 if ( sameTableConfig.getNodes().isEmpty() &&
		 * sameTableConfig.getRelactions().size() > 0) { return ; }
		 */
		if (lstKeys.isEmpty()) {
			return;
		}
		long start = System.currentTimeMillis();
		int parallel = labelIndexConfig.getParallel();
		List<List<String[]>> lstBatchKeys = null;

		if (lstKeys.size() < parallel) {
			lstBatchKeys = new ArrayList<List<String[]>>();
			lstBatchKeys.add(lstKeys);
		} else {
			lstBatchKeys = new ArrayList<List<String[]>>(parallel);
			for (int i = 0; i < parallel; i++) {
				List<String[]> lst = new ArrayList<String[]>();
				lstBatchKeys.add(lst);
			}

			for (int i = 0; i < lstKeys.size(); i++) {
				lstBatchKeys.get(i % parallel).add(lstKeys.get(i));
			}
		}

		List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

		ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
		builder.setNameFormat("ParallelBatchQuery");
		ThreadFactory factory = builder.build();
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(lstBatchKeys.size(), factory);

		for (List<String[]> keys : lstBatchKeys) {
			Callable<Integer> callable = new Neo4jMappingDataBatchPutCallable(conn, tname, labelIndexConfig, keys);
			FutureTask<Integer> future = (FutureTask<Integer>) executor.submit(callable);
			futures.add(future);
		}
		executor.shutdown();

		// Wait for all the tasks to finish
		try {
			boolean stillRunning = !executor.awaitTermination(600000, TimeUnit.MILLISECONDS);
			if (stillRunning) {
				try {
					executor.shutdownNow();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (InterruptedException e) {
			try {
				Thread.currentThread().interrupt();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}

		int save_total = 0;
		// Look for any exception
		for (Future<Integer> f : futures) {
			try {
				if (f != null && f.get() != null) {
					save_total += (Integer) f.get();
				}
			} catch (InterruptedException e) {
				try {
					Thread.currentThread().interrupt();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		// logger.info("<" + "node put" + "> \t spend [" +
		// (System.currentTimeMillis() - start) + "]");

		String message = "nodes \t [%s] \t relactions [%s] \t " + type + " put \t [%d] \t spend [%d]";
		logger.info(String.format(message, sameTableConfig.getNode_names(), sameTableConfig.getRelaction_names(),
				save_total, (System.currentTimeMillis() - start)));

	}

	/**
	 * 保存neo4j 对应node的ID值
	 * 
	 * @param mapNodeData
	 *            key:Neo4jNodeDataInfo.labelRowkey
	 */
	public void saveNeo4jLabelIndex(ConcurrentHashMap<String, Neo4jNodeDataInfo> mapNodeData,
			SameTableConfig sameTableConfig) {
		/*
		 * //如果当前只插入关系数据，不保存node数据 if ( sameTableConfig.getNodes().isEmpty() &&
		 * sameTableConfig.getRelactions().size() > 0) { return ; }
		 */

		long start = System.currentTimeMillis();
		int parallel = labelIndexConfig.getParallel();
		List<List<Neo4jNodeDataInfo>> lstBatchKeys = null;

		List<Neo4jNodeDataInfo> lstKeys = new ArrayList<Neo4jNodeDataInfo>();
		lstKeys.addAll(mapNodeData.values());

		if (mapNodeData.size() < parallel) {
			lstBatchKeys = new ArrayList<List<Neo4jNodeDataInfo>>();

			lstBatchKeys.add(lstKeys);
		} else {
			lstBatchKeys = new ArrayList<List<Neo4jNodeDataInfo>>(parallel);
			for (int i = 0; i < parallel; i++) {
				List<Neo4jNodeDataInfo> lst = new ArrayList<Neo4jNodeDataInfo>();
				lstBatchKeys.add(lst);
			}

			for (int i = 0; i < lstKeys.size(); i++) {
				lstBatchKeys.get(i % parallel).add(lstKeys.get(i));
			}
		}

		List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

		ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
		builder.setNameFormat("ParallelBatchQuery");
		ThreadFactory factory = builder.build();
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(lstBatchKeys.size(), factory);

		for (List<Neo4jNodeDataInfo> keys : lstBatchKeys) {
			Callable<Integer> callable = new Neo4jNodeDataBatchPutCallable(conn, labelIndexConfig, keys);
			FutureTask<Integer> future = (FutureTask<Integer>) executor.submit(callable);
			futures.add(future);
		}
		executor.shutdown();

		// Wait for all the tasks to finish
		try {
			boolean stillRunning = !executor.awaitTermination(300000, TimeUnit.MILLISECONDS);
			if (stillRunning) {
				try {
					executor.shutdownNow();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (InterruptedException e) {
			try {
				Thread.currentThread().interrupt();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}

		int save_total = 0;
		// Look for any exception
		for (Future<Integer> f : futures) {
			try {
				if (f != null && f.get() != null) {
					save_total += (Integer) f.get();
				}
			} catch (InterruptedException e) {
				try {
					Thread.currentThread().interrupt();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		// logger.info("<" + "node put" + "> \t spend [" +
		// (System.currentTimeMillis() - start) + "]");

		String message = "nodes \t [%s] \t relactions [%s] \t node put \t [%d] \t spend [%d]";
		logger.info(String.format(message, sameTableConfig.getNode_names(), sameTableConfig.getRelaction_names(),
				save_total, (System.currentTimeMillis() - start)));

	}

	public void createNeo4jLabelIndexTable() throws IOException {
		Admin admin = conn.getAdmin();
		long start = System.currentTimeMillis();
		logger.info("开始重新创建node对应ID表==================");

		// 表存在
		TableName tableName = TableName.valueOf(labelIndexConfig.getNodeTable());

		if (admin.tableExists(tableName)) {
			try {
				admin.disableTable(tableName);
			} catch (Exception e) {
				logger.error(e);
			}
			admin.deleteTable(tableName);
		}

		HTableDescriptor hbaseTable = new HTableDescriptor(tableName);
		hbaseTable.addFamily(new HColumnDescriptor(labelIndexConfig.getFamily()));

		String partions = labelIndexConfig.getPartions();
		if (partions != null && partions.length() > 0) {
			String[] splitPartions = partions.split(",");
			int length = splitPartions.length;
			byte[][] splitKeys = new byte[length][];
			for (int i = 0; i < length; i++) {
				splitKeys[i] = Bytes.toBytes(splitPartions[i]);
			}
			admin.createTable(hbaseTable, splitKeys);
		} else {
			admin.createTable(hbaseTable);
		}
		logger.info("结束重新创建node对应ID表，耗时：[" + (System.currentTimeMillis() - start) + "]");

		admin.close();
	}

	/****************************************
	 * xdk
	 *****************************************/

	/**
	 * 查询neo4j node对应的ID值
	 * 
	 * @param mapNodeData
	 *            key:Neo4jNodeDataInfo.labelRowkey
	 * @return
	 */
	public void getNeo4jMappingIndex(String tname, Map<String, Long> mapNodeData, SameTableConfig sameTableConfig,
			String type) {

		long start = System.currentTimeMillis();
		if (mapNodeData.keySet().size() == 0) {
			return;
		}

		// 如果是关系,判断关系是否需要从表中获取已经插入的关系
		if ("relaction".equalsIgnoreCase(type)) {
			if (!sameTableConfig.getRelactions().get(0).isNode_get()) {
				String message = "nodes \t [%s] \t relactions [%s] \t " + type + " not get ";
				logger.info(
						String.format(message, sameTableConfig.getNode_names(), sameTableConfig.getRelaction_names()));
				return;
			}
		}

		Map<String, String> searchMap = this.getNeo4jMappingIndex(tname, mapNodeData.keySet());
		int search_count = searchMap.size();
		Iterator<Entry<String, String>> iterator = searchMap.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, String> nextEntry = iterator.next();
			mapNodeData.put(nextEntry.getKey(), Long.parseLong(nextEntry.getValue()));
		}
		String message = "nodes \t [%s] \t relactions [%s] \t " + type + " get \t [%d] \t spend [%d]";
		logger.info(String.format(message, sameTableConfig.getNode_names(), sameTableConfig.getRelaction_names(),
				search_count, (System.currentTimeMillis() - start)));

	}

	public Map<String, String> getNeo4jMappingIndex(String tname, Set<String> lstKeys) {
		Map<String, String> hashRet = new HashMap<String, String>();
		if (!lstKeys.isEmpty()) {
			int parallel = labelIndexConfig.getParallel();
			List<Set<String>> lstBatchKeys = null;
			if (lstKeys.size() < parallel) {
				lstBatchKeys = new ArrayList<Set<String>>(1);
				lstBatchKeys.add(lstKeys);
			} else {
				lstBatchKeys = new ArrayList<Set<String>>(parallel);
				for (int i = 0; i < parallel; i++) {
					Set<String> lst = new HashSet<String>();
					lstBatchKeys.add(lst);
				}

				Iterator<String> iterator = lstKeys.iterator();
				int i = 0;
				while (iterator.hasNext()) {
					lstBatchKeys.get(i % parallel).add(iterator.next());
					i++;
				}
			}

			List<Future<Map<String, String>>> futures = new ArrayList<Future<Map<String, String>>>();
			ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
			builder.setNameFormat("ParallelBatchQuery");
			ThreadFactory factory = builder.build();
			ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(lstBatchKeys.size(),
					factory);

			for (Set<String> keys : lstBatchKeys) {
				Callable<Map<String, String>> callable = new Neo4jMappingDataBatchGetCallable(conn, tname,
						labelIndexConfig, keys);
				FutureTask<Map<String, String>> future = (FutureTask<Map<String, String>>) executor.submit(callable);
				futures.add(future);
			}
			executor.shutdown();

			// Wait for all the tasks to finish
			try {
				boolean stillRunning = !executor.awaitTermination(600000, TimeUnit.MILLISECONDS);
				if (stillRunning) {
					try {
						executor.shutdownNow();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} catch (InterruptedException e) {
				try {
					Thread.currentThread().interrupt();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

			// Look for any exception
			for (Future<Map<String, String>> f : futures) {
				try {
					if (f != null && f.get() != null) {
						hashRet.putAll(f.get());
					}
				} catch (InterruptedException e) {
					try {
						Thread.currentThread().interrupt();
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
		}

		return hashRet;
	}

	public void createNeo4jIndexTable() throws IOException {
		Admin admin = conn.getAdmin();
		createNeo4jMappingIndexTable(admin, labelIndexConfig.getNodeTable(), labelIndexConfig.getFamily(), "Node");
		createNeo4jMappingIndexTable(admin, labelIndexConfig.getRelationshipTable(), labelIndexConfig.getFamily(),
				"Relation");
		admin.close();
	}

	private void createNeo4jMappingIndexTable(Admin admin, String tname, String family, String title)
			throws IOException {
		long start = System.currentTimeMillis();
		logger.info("开始重新创建" + title + "对应ID表==================");
		// 表存在
		TableName tableName = TableName.valueOf(tname);
		if (admin.tableExists(tableName)) {
			try {
				admin.disableTable(tableName);
			} catch (Exception e) {
			}
			admin.deleteTable(tableName);
		}
		HTableDescriptor hbaseTable = new HTableDescriptor(tableName);
		hbaseTable.addFamily(new HColumnDescriptor(family));
		String partions = labelIndexConfig.getPartions();
		if (partions != null && partions.length() > 0) {
			String[] splitPartions = partions.split(",");
			int length = splitPartions.length;
			byte[][] splitKeys = new byte[length][];
			for (int i = 0; i < length; i++) {
				splitKeys[i] = Bytes.toBytes(splitPartions[i]);
			}
			admin.createTable(hbaseTable, splitKeys);
		} else {
			admin.createTable(hbaseTable);
		}
		logger.info("结束重新创建" + title + "对应ID表，耗时：[" + (System.currentTimeMillis() - start) + "]");
	}

}
