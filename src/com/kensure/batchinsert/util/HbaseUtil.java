package com.kensure.batchinsert.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class HbaseUtil {

	private static final Logger logger = Logger.getLogger(HbaseUtil.class);

	public static final Configuration initConfiguration() {
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "192.168.10.82");
		return configuration;
	}

	public static final boolean deleteTable(Admin admin, String tableName) {
		try {
			TableName htableName = TableName.valueOf(tableName);
			if (admin.isTableAvailable(htableName)) {
				if (!admin.isTableDisabled(htableName)) {
					admin.disableTable(htableName);
				}
				admin.deleteTable(htableName);
				logger.info(String.format("删除表[%s]", tableName));
			}
			return true;
		} catch (Exception e) {
			logger.error(e);
		}
		return false;

	}

	public static final boolean deleteSnapshot(Admin admin, String snapshotName) {
		try {
			admin.deleteSnapshot(snapshotName);
			logger.info(String.format("删除快照[%s]", snapshotName));
			return true;
		} catch (Exception e) {
			logger.error(e);
		}
		return false;

	}

	public static final boolean snapshot(Admin admin, String tableName) {
		try {

			String snapshots = String.format("snapshots_%s", tableName.replaceAll("[:]", "_"));
			if(admin.listSnapshots(snapshots).size()>0) {
				deleteSnapshot(admin, snapshots);
			}
			admin.snapshot(snapshots, TableName.valueOf(tableName));
			logger.info(String.format("表[%s]生成的快照[%s]", tableName, snapshots));
			return true;
		} catch (Exception e) {
			logger.error(e);
		}
		return false;
	}

	public static final void deleteSnapshot(Admin admin, Set<String> tableNames) {
		try {
			List<SnapshotDescription> listSnapshots = admin.listSnapshots();
			for (SnapshotDescription snapshotDescription : listSnapshots) {
				if (tableNames.contains(snapshotDescription.getTable())) {
					deleteSnapshot(admin, snapshotDescription.getName());
				}
			}
		} catch (Exception e) {
			logger.error(e);
		}
	}

	public static final void restoreSnapshot(Admin admin, Set<String> tableNames) {
		Set<String> tables = new HashSet<String>();
		List<SnapshotDescription> listSnapshots = null;
		try {
			listSnapshots = admin.listSnapshots();
		} catch (Exception e) {
			logger.error(e);
		}
		if (listSnapshots != null) {
			for (SnapshotDescription snapshotDescription : listSnapshots) {
				try {
					String tableName = snapshotDescription.getTable();
					String snapshotName = snapshotDescription.getName();
					if (tableNames.contains(tableName)) {
						if (!tables.contains(tableName)) {
							tables.add(tableName);
							TableName htableName = TableName.valueOf(tableName);
							if (admin.tableExists(htableName)) {
								logger.info(String.format("表[%s]存在", tableName));
								if (!admin.isTableDisabled(htableName)) {
									admin.disableTable(htableName);
									logger.info(String.format("设置表[%s]为只读状态", tableName));
								}
							} else {
								logger.info(String.format("表[%s]不存在", tableName));
							}
							admin.restoreSnapshot(snapshotDescription.getName());
							if (admin.isTableDisabled(htableName)) {
								admin.enableTable(htableName);
								logger.info(String.format("设置表[%s]为读写状态", tableName));
							}
							logger.info(String.format("表[%s]从快照中[%s]中恢复", tableName, snapshotName));
							deleteSnapshot(admin, snapshotName);
						}
					}
				} catch (Exception e) {
					logger.error(e);
				}
			}
		}

	}

	public static void main(String[] args) throws IOException {
		Connection connection = ConnectionFactory.createConnection(initConfiguration());
		/*Table table = connection.getTable(TableName.valueOf("neo4j:node_index_xdk"));
		List<Get> lstGet = new ArrayList<Get>();
		lstGet.add(new Get(Bytes.toBytes("00_130535197205209130")));
		lstGet.add(new Get(Bytes.toBytes("00_130535197205209131")));
		Result[] results = table.get(lstGet);
		for (Result result : results) {
			System.out.println(result);
		}*/
		Admin admin = connection.getAdmin();
		snapshot(admin, "neo4j:relationship_index_xdk");
		deleteSnapshot(admin, "snapshots_neo4j_relationship_index_xdk");
		//List<SnapshotDescription> listSnapshots = admin.listSnapshots("snapshots_*");
		//System.out.println(listSnapshots.size());
		admin.close();
		connection.close();
	}
}
