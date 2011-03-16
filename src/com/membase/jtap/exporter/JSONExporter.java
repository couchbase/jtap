package com.membase.jtap.exporter;

import com.membase.jtap.exception.FieldDoesNotExistException;
import com.membase.jtap.message.ResponseMessage;

/**
 * Prints tap message information out as a json to standard out.
 */
public class JSONExporter implements Exporter {
	
	/**
	 * Creates a JSONExporter.
	 */
	public JSONExporter() {

	}
	
	/**
	 * Writes a tap messages key name and attempts to write its value if one exists.
	 * @param message The tap message to export data from.
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
		System.out.println("{" + key + ":" + value + "}");
	}

	@Override
	public void close() {
		
	}
}
