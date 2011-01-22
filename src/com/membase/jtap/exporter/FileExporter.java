package com.membase.jtap.exporter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileExporter implements Exporter {
	private static final Logger LOG = LoggerFactory.getLogger(FileExporter.class);
	
	FileWriter fstream;
	BufferedWriter out;
	
	public FileExporter(String path) {
		 try{
			fstream = new FileWriter(path);
			out = new BufferedWriter(fstream);
		} catch (IOException e) {
			LOG.info("Cannot open file " + path);
		}
	}
	
	@Override
	public void write(String key) {
		try {
			out.write("Key: " + key + "\n");
		} catch (IOException e) {
			LOG.info("Could not write key " + key + " to file");
		}
	}

	@Override
	public void write(String key, String value) {
		try {
			out.write("Key: " + key + "\n");
			out.write("Value: " + value + "\n");
		} catch (IOException e) {
			LOG.info("Could not write key " + key + " and its value to file");
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
