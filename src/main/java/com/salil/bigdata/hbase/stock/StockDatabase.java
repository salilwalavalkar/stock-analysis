package com.salil.bigdata.hbase.stock;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * HBase data access class for Stock table.
 * 
 * @author salil
 *
 */
public class StockDatabase {

	// Schema Constants - table, column family and column names
	public final static byte[] TABLE_NAME = "stock".getBytes();
	public final static byte[] COLUMN_FAMILY = "d".getBytes();
	public final static byte[] COL_OPEN = "open".getBytes();
	public final static byte[] COL_HIGH = "high".getBytes();
	public final static byte[] COL_LOW = "low".getBytes();
	public final static byte[] COL_CLOSE = "close".getBytes();
	public final static byte[] COL_VOLUME = "volume".getBytes();

	public enum CustomFilter {
		PREFIX {
			/**
			 * Match using binary comparison. Equals, Not equals, Greater than etc. 
			 */
			@Override
			Filter getFilter(String compareValue) {
				return new PrefixFilter(Bytes.toBytes(compareValue));
			}
		},		
		BINARY {
			/**
			 * Match using binary comparison. Equals, Not equals, Greater than etc. 
			 */
			@Override
			Filter getFilter(String compareValue) {
				return new RowFilter(CompareFilter.CompareOp.EQUAL,
						new BinaryComparator(Bytes.toBytes(compareValue)));
			}
		},
		REGEX {
			/**
			 * Match using regular expression. 
			 */
			@Override
			Filter getFilter(String compareValue) {
				return new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(compareValue));
			}
		},
		SUBSTR {
			/**
			 * Match using substring. 
			 */
			@Override
			Filter getFilter(String compareValue) {
				return new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(compareValue));
			}
		};		

		abstract Filter getFilter(String compareValue);
	}

	// HBase configuration
	private final Configuration config;

	public Configuration getConfig() {
		return config;
	}

	public StockDatabase(Configuration inConfig) {
		config = inConfig;
	}

	/**
	 * Creates the HBase table.
	 */
	public void createTable() throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(config); Admin admin = connection.getAdmin()) {

			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(COLUMN_FAMILY));

			if (!admin.tableExists(table.getTableName())) {
				System.out.print("Creating table: " + table.getNameAsString());
				admin.createTable(table);
				System.out.println(" Done.");
			}
		}
	}

	/**
	 * Drops the HBase table.
	 */
	public void dropTable() throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(config); Admin admin = connection.getAdmin()) {

			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(COLUMN_FAMILY));

			if (admin.tableExists(table.getTableName())) {
				System.out.print("Dropping table: " + table.getNameAsString());
				// Table must be disabled before it can be dropped
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
				System.out.println(" Done.");
			}
		}
	}

	/**
	 * Creates a rowkey from stock name and date.
	 */
	static byte[] constructKey(String stock, String date) {
		// Rowkey optimized so that stocks are close to each other.
		return (stock + "|" + date).getBytes();
	}

	/**
	 * Gets a single row given the date and stock symbol.
	 */
	public void getRow(String symbol, String date) throws IOException {
		try (Connection conn = ConnectionFactory.createConnection(config)) {
			// Get the table
			Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
			// Construct a "getter" with the rowkey.
			Get get = new Get(constructKey(symbol, date));
			// Get the result by passing the getter to the table
			Result r = table.get(get);
			// return the results
			if (r.isEmpty()) {
				return;
			}
			printRowData(r);
		}
	}

	/**
	 * Gets a single cell given the date and stock symbol and column Id.
	 */
	public String getCell(String date, String symbol, byte[] column) throws IOException {
		try (Connection conn = ConnectionFactory.createConnection(config)) {
			// Get the table
			Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
			// Construct a "getter" with the rowkey.
			Get get = new Get(constructKey(symbol, date));
			// Further refine the "get" with a column specification
			get.addColumn(COLUMN_FAMILY, column);
			// Get the result by passing the getter to the table
			Result r = table.get(get);
			// return the results
			if (r.isEmpty()) {
				return null;
			}
			// Gets the value of the first (and only) column
			return new String(r.value());
		}
	}

	/**
	 * Specifies a range of rows to retrieve based on a starting and ending row
	 * key.
	 */
	public void getRows(String symbol, String startDate, String endDate) throws IOException {
		ResultScanner results = null;
		try (Connection conn = ConnectionFactory.createConnection(config)) {
			// Get the table
			Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
			// Create the scan
			Scan scan = new Scan();
			// start at a specific rowkey.
			scan.setStartRow(constructKey(symbol, startDate));
			scan.setStopRow(constructKey(symbol, endDate));
			// Get the scan results
			results = table.getScanner(scan);
			int cnt = 1;
			for (Result r : results) {
				System.out.println("Record no: " + (cnt++));
				printRowData(r);
			}
		} finally {
			// ResultScanner must be closed.
			if (results != null)
				results.close();
		}
	}

	/**
	 * Specifies a range of rows to retrieve based on a starting row key and
	 * retrieves up to limit rows.
	 */
	public void getRows(String symbol, String startDate, int limit) throws IOException {
		ResultScanner results = null;
		try (Connection conn = ConnectionFactory.createConnection(config)) {
			// Get the table
			Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
			// Create the scan
			Scan scan = new Scan();
			// start at a specific rowkey.
			scan.setStartRow(constructKey(symbol, startDate));
			// Cache only limited rows.
			scan.setCaching(limit);
			// Set a server side inbuilt filter for pagination.
			scan.setFilter(new PageFilter(limit));
			// Get the scan results
			results = table.getScanner(scan);
			int cnt = 1;
			for (Result r : results) {
				System.out.println("Record no: " + (cnt++));
				printRowData(r);
			}
		} finally {
			// ResultScanner must be closed.
			if (results != null)
				results.close();
		}
	}

	/**
	 * Specifies a range of rows matching the given prefix for row key.
	 */
	public void getRows(String compareValue, CustomFilter customFilter) throws IOException {
		ResultScanner results = null;
		try (Connection conn = ConnectionFactory.createConnection(config)) {
			// Get the table
			Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
			// Create the scan
			Scan scan = new Scan();
			scan.setFilter(customFilter.getFilter(compareValue));
			// Get the scan results
			results = table.getScanner(scan);
			int cnt = 1;
			for (Result r : results) {
				System.out.println("Record no: " + (cnt++));
				printRowData(r);
			}
		} finally {
			// ResultScanner must be closed.
			if (results != null)
				results.close();
		}
	}

	/**
	 * Specifies a range of rows matching the given string in a column value.
	 */
	public void getRowsSingleColumnFilter(String compareValue, byte[] columnFamily, byte[] qualifier) throws IOException {
		ResultScanner results = null;
		try (Connection conn = ConnectionFactory.createConnection(config)) {
			// Get the table
			Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
			// Create the scan
			Scan scan = new Scan();
			SingleColumnValueFilter columnFilter = new SingleColumnValueFilter(columnFamily, qualifier,
					CompareFilter.CompareOp.EQUAL, new SubstringComparator(compareValue));
			columnFilter.setFilterIfMissing(true);			
			scan.setFilter(columnFilter);
			// Get the scan results
			results = table.getScanner(scan);
			int cnt = 1;
			for (Result r : results) {
				System.out.println("Record no: " + (cnt++));
				printRowData(r);
			}
		} finally {
			// ResultScanner must be closed.
			if (results != null)
				results.close();
		}
	}
	
	/**
	 * Print complete row data including all column families and values.
	 */
	private void printRowData(Result result) {
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
		System.out.println("Rowkey: " + new String(result.getRow()));
		for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMapEntry : map.entrySet()) {
			String family = Bytes.toString(navigableMapEntry.getKey());
			System.out.println("Family: " + family);
			NavigableMap<byte[], NavigableMap<Long, byte[]>> familyContents = navigableMapEntry.getValue();
			System.out.println("Qualifiers: ");
			for (Map.Entry<byte[], NavigableMap<Long, byte[]>> mapEntry : familyContents.entrySet()) {
				String qualifier = Bytes.toString(mapEntry.getKey());
				System.out.print(qualifier);
				NavigableMap<Long, byte[]> qualifierContents = mapEntry.getValue();
				for (Map.Entry<Long, byte[]> entry : qualifierContents.entrySet()) {
					Long timestamp = entry.getKey();
					String value = Bytes.toString(entry.getValue());
					System.out.printf("\t%s, %d\n", value, timestamp);
				}
			}
		}
		System.out.println("------------------------------------");
	}
}
