package com.salil.bigdata.hbase.stock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Runnable class to test Hbase operations.
 * 
 * @author salil
 *
 */
public class TestHBase {

	public static void main(String... args) throws IOException {

		// HBase context from configuration file
		Configuration config = HBaseConfiguration.create();

		// StockDatabase to be used as a DAO for the stock price data.
		StockDatabase db = new StockDatabase(config);

		// Drop and add the table.
		deleteExistingTables(db);

		// Import the sample data.
		importCSVData(db, "rawdata/GOOG.csv", "GOOG");
		importCSVData(db, "rawdata/AAPL.csv", "AAPL");

		// Get a single cell.
		System.out
				.println("GOOG 5/16 close is " + db.getCell("2016-05-16", "GOOG", "close"));

		// Get a single row.
		db.getRow("GOOG", "2016-05-16");

		// Get multiple scan rows based on start and end dates.
		db.getRows("GOOG", "2016-05-16", "2016-05-20");

		// Get scan rows based on start date while limiting number of records.
		db.getRows("GOOG", "2016-05-16", 3);

		// Get scan rows based on prefix filter for rowkey.
		db.getRows("GOOG|2016-05", StockDatabase.CustomFilter.PREFIX);

		// Get scan rows based on binary filter for equals rowkey.
		db.getRows("GOOG|2016-05-16", StockDatabase.CustomFilter.BINARY);

		// Get scan rows based on regex filter for equals rowkey.
		db.getRows(".*-05-16", StockDatabase.CustomFilter.REGEX);

		// Get scan rows based on substring filter for equals rowkey.
		db.getRows("2016-05-16", StockDatabase.CustomFilter.SUBSTR);

		// Get rows matching column name and column value.
		Map<String, String> qualifierVsColumnvalueMap = new HashMap<String, String>();
		qualifierVsColumnvalueMap.put("close", "716.49");
		qualifierVsColumnvalueMap.put("open", "709.13");
		db.getRowsMultiColumnFilter(qualifierVsColumnvalueMap);

	}

	/**
	 * Delete existing tables.
	 */
	private static void deleteExistingTables(StockDatabase db) throws IOException {
		db.dropTable();
		db.createTable();
	}

	/**
	 * Imports CSV data.
	 */
	private static void importCSVData(StockDatabase db, String filename, String symbol) {
		System.out.println("Loading data for " + symbol + " from " + filename);
		try (LineReader reader = new LineReader(filename, new LineProcessor(symbol, db.getConfig()))) {
			reader.readData();
			System.out.println("Loading complete.");
		} catch (Exception e) {
			System.out.println("Caught exception while importing data:" + e.getMessage());
			e.printStackTrace();
		}
	}
}