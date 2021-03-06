package com.membase.jtap.exporter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.membase.jtap.exception.FieldDoesNotExistException;
import com.membase.jtap.message.ResponseMessage;

/**
 * Writes information from a tap stream to a specified file.
 */
public class FileExporter implements Exporter {
	private static final Logger LOG = LoggerFactory.getLogger(FileExporter.class);
	private FileWriter fstream;
	private BufferedWriter out;
	
	/**
	 * Creates a file exporter.
	 * @param path The path of the file to write data to.
	 */
	public FileExporter(String path) {
		 try{
			fstream = new FileWriter(path);
			out = new BufferedWriter(fstream);
		} catch (IOException e) {
			LOG.info("Cannot open file " + path);
		}
	}
	
	/**
	 * Writes the key name in the tap message an attempts to write the value
	 * if it exists.
	 * @param The tap message to export.
	 */
	@Override
	public void write(ResponseMessage message) {
		String key;
		String value;
		try {
			key = message.getKey();
		} catch (FieldDoesNotExistException e) {
			key = null;
		}
		try {
			value = message.getValue();
		} catch (FieldDoesNotExistException e) {
			value = null;
		}
		try {
			out.write("Key: " + key + ", Value: " + value + "\n");
		} catch (IOException e) {
			LOG.info("Could not write key " + key + " to file");
		}
	}

	@Override
	public void close() {
		try {
			out.close();
			fstream.close();
		} catch (IOException e) {
			LOG.info("Could not close export file");
		}
		
	}
	
}
