package com.kensure.batchinsert.test;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

import com.kensure.batchinsert.util.HbaseConfigUtil;
import com.kensure.batchinsert.util.LoginUtil;



/**
 * HBase Development Instruction Sample Code The sample code uses user
 * information as source data,it introduces how to implement businesss process
 * development using HBase API
 */
public class MyHBaseSample {
	  private final static Log LOG = LogFactory.getLog(MyHBaseSample.class.getName());

	
	 
	
  	public static void main( String[] args ) throws IOException {
  		
  		
  	    Connection conn = ConnectionFactory.createConnection(HbaseConfigUtil.getHabseConfiguration());
  		
  	    // Specify the column family name.
  	    byte[] familyName = Bytes.toBytes("D");
  	    // Specify the column name.
  	    byte[][] qualifier = { Bytes.toBytes("ZJHM"), Bytes.toBytes("ZJXY") };
  	    // Specify RowKey.
  	    byte[] rowKey = Bytes.toBytes("00_000000191701010000");

  	    Table table = null;
  	    try {
  	      // Create the Configuration instance.
  	      table = conn.getTable( TableName.valueOf("masjwy_ywk:d_people_label"));

  	      // Instantiate a Get object.
  	      Get get = new Get(rowKey);

  	      // Set the column family name and column name.
  	      get.addColumn(familyName, qualifier[0]);
  	      get.addColumn(familyName, qualifier[1]);

  	      // Submit a get request.
  	      Result result = table.get(get);

  	      
  	    String rowkey = Bytes.toString(result.getRow());
		String labelIndex = Bytes.toString(result.getValue(
				Bytes.toBytes("D"),
				Bytes.toBytes("ZJHM")));
  	      
  	      // Print query results.
  	      for (Cell cell : result.rawCells()) {
  	        LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
  	            + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
  	            + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
  	            + Bytes.toString(CellUtil.cloneValue(cell)));
  	      }
  	      LOG.info("Get data successfully.");
  	    } catch (IOException e) {
  	      LOG.error("Get data failed ", e);
  	    } finally {
  	      if (table != null) {
  	        try {
  	          // Close the HTable object.
  	          table.close();
  	        } catch (IOException e) {
  	          LOG.error("Close table failed ", e);
  	        }
  	      }
  	    }
  	    LOG.info("Exiting testGet.");
  	    		
  	}
}
