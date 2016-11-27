package lt.hbase;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;

public class HbaseConnection {
	private String rootDir;
	private String zkServer;
	private String port;
	
	Configuration conf;
	HConnection hConn=null;
	
	HbaseConnection(String rootDir,String zkServer,String port) throws IOException{
		this.rootDir=rootDir;
		this.zkServer=zkServer;
		this.port=port;
		conf=HBaseConfiguration.create();
		conf.set("hbase.roodir", rootDir);
		conf.set("hbase.zookeeper.quorum", zkServer);
		conf.set("hbase.zookeeper.property.clientPort", port);
		hConn=HConnectionManager.createConnection(conf);
	}
	
	public void createTable(String tableName,List<String> cols) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		HBaseAdmin admin=new HBaseAdmin(conf);
		if(admin.tableExists(tableName)){
			throw new IOException("table exists");
		}
		else{
			HTableDescriptor tableDesc=new HTableDescriptor(tableName);
			for(String col:cols){
				HColumnDescriptor colDesc=new HColumnDescriptor(col);
				colDesc.setCompressionType(Algorithm.GZ);
				colDesc.setDataBlockEncoding(DataBlockEncoding.DIFF);
				tableDesc.addFamily(colDesc);
			}
			admin.createTable(tableDesc);
		}
		admin.close();
		
	}
	
	public void saveData(String tableName,List<Put> puts){
		
	}
	
	public Result getData(String tableName,String rowKey){
		
		
		
		
		return null;
		
	}
	
	
	
	public static void main(String[] args) throws IOException {
		String rootDir="hdfs://192.168.1.106:9000/hbase";
		String zkServer="192.168.1.106";
		String port="2181";
		
		HbaseConnection conn=new HbaseConnection(rootDir,zkServer,port);
		List<String> cols=new LinkedList<String>();
		cols.add("basicInfo");
		cols.add("moreInfo");
		conn.createTable("students", cols);
	}
	
}
