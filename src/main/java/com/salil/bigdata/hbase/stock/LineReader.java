package com.salil.bigdata.hbase.stock;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;

/**
 * 
 * Read a text file line by line.
 * 
 * @author salil
 *
 */
public class LineReader implements AutoCloseable {

	private final String filename;
	private LineProcessor lineProcessor;

	public LineReader(String inFilename, LineProcessor lineProcessor) {
		filename = inFilename;
		this.lineProcessor = lineProcessor;
	}

	/**
	 * Reads through a text file line by line and process it using line
	 * processor class.
	 */
	public void readData() throws FileNotFoundException, IOException {
		if (filename == null || lineProcessor == null)
			return;
		File f = new File(filename);
		if (!f.exists() || f.isDirectory())
			return;

		try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
			String line;
			while ((line = reader.readLine()) != null) {
				lineProcessor.processData(line);
			}
		}
	}

	/**
	 * On close, execute close on the line processor too.
	 */
	@Override
	public void close() throws Exception {
		if (lineProcessor != null)
			lineProcessor.close();
	}
}