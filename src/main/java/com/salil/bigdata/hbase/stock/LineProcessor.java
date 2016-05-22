package com.salil.bigdata.hbase.stock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

/**
 * 
 * Imports bulk data into HBase table.
 * 
 * @author salil
 *
 */

public class LineProcessor implements AutoCloseable {

	// Data members
	private final String currentStock;
	private final List<Put> currentImport; // Bulk records for inserting.
	private boolean skipFirst; // True if there is a header to skip.
	private Configuration config;

	public LineProcessor(String inSymbol, boolean inSkipFirst, Configuration config) {
		skipFirst = inSkipFirst;
		currentStock = inSymbol;
		currentImport = new ArrayList<Put>();
		this.config = config;
	}

	public LineProcessor(String inSymbol, Configuration config) {
		this(inSymbol, true, config); // Default true, assumes header.
	}

	/**
	 * Import a CSV line from the data source and parse it.
	 */
	public void processData(String line) throws IOException {
		if (line == null)
			return;
		// If true, skip the header
		if (skipFirst) {
			skipFirst = false;
			return;
		}

		// Split the line.
		String[] data = line.split(",");
		if (data.length != 6)
			return;

		// Construct a "put" object for insert
		Put p = new Put(StockDatabase.constructKey(currentStock, data[0]));
		p.addColumn(StockDatabase.COLUMN_FAMILY, StockDatabase.COL_OPEN, data[1].getBytes());
		p.addColumn(StockDatabase.COLUMN_FAMILY, StockDatabase.COL_HIGH, data[2].getBytes());
		p.addColumn(StockDatabase.COLUMN_FAMILY, StockDatabase.COL_LOW, data[3].getBytes());
		p.addColumn(StockDatabase.COLUMN_FAMILY, StockDatabase.COL_CLOSE, data[4].getBytes());
		p.addColumn(StockDatabase.COLUMN_FAMILY, StockDatabase.COL_VOLUME, data[5].getBytes());

		// Cache the "put" object for bulk load.
		currentImport.add(p);
	}

	/**
	 * Imports bulk data into HBase table.
	 */
	@Override
	public void close() throws Exception {
		if (currentImport.isEmpty())
			return;
		try (Connection conn = ConnectionFactory.createConnection(config)) {
			Table table = conn.getTable(TableName.valueOf(StockDatabase.TABLE_NAME));
			table.put(currentImport);
			table.close();
		} finally {
			currentImport.clear();
		}
	}
}