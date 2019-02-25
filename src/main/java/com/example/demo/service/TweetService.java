package com.example.demo.service;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TweetService {
	private final static String HMASTER = "namenode004.cluster02.bt";
	private final static String ZOOKEEPER = "master001.cluster02.bt,namenode004.cluster02.bt,namenode005.cluster02.bt";
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	private Configuration config;
	
	public void config() {
		 config = HBaseConfiguration.create();
		    config.set("hbase.master", HMASTER);
		    config.setInt("timeout", 5000);
		    config.set("hbase.zookeeper.quorum", ZOOKEEPER);
		    config.set("zookeeper.znode.parent", "/hbase-unsecure2");
	}
	
	public List<Cell> scanRegexRowKey(String tableName, String regexKey) {
		config();
		try (Connection connection = ConnectionFactory.createConnection(config);
		     Table table = connection.getTable(TableName.valueOf(tableName))) {
			Scan scan = new Scan();
			Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regexKey));
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				return r.listCells();
			}
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		return null;
	}
	
}
