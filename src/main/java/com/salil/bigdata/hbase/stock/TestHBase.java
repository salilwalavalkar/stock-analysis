package com.salil.bigdata.hbase.stock;

import java.io.IOException;

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

//		// Drop and add the table.
//		deleteExistingTables(db);
//
//		// Import the sample data.
//		importCSVData(db, "rawdata/GOOG.csv", "GOOG");
//		importCSVData(db, "rawdata/AAPL.csv", "AAPL");

		// Get a single cell.
		System.out.println("GOOG 5/16 close is " + db.getCell("2016-05-16", "GOOG", StockDatabase.COL_CLOSE));

		// Get a single row.
		db.getRow("GOOG", "2016-05-16");

		// Get multiple scan rows based on start and end dates.
		db.getRows("GOOG", "2016-05-16", "2016-05-20");

		// Get scan rows based on start date while limiting number of records.
		db.getRows("GOOG", "2016-05-16", 3);
		
		// Get scan rows based on prefix filter for rowkey.
		db.getRows("GOOG|2016-05");
		
		//TODO: Other rowkey based filters.
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