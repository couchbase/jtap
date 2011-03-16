package com.membase.jtap.exporter;

import com.membase.jtap.exception.FieldDoesNotExistException;
import com.membase.jtap.message.ResponseMessage;

/**
 * Exports keys and their value to standard out.
 */
public class TextExporter implements Exporter {
	
	public TextExporter() {

	}
	
	/**
	 * Prints a key name and attempts to print its value if it exists to
	 * standard out.
	 */
	@Override
	public void write(ResponseMessage message) {
		try {
			System.out.println(message.getKey());
		} catch (FieldDoesNotExistException e) {
		}
		try {
			System.out.println(message.getValue());
		} catch (FieldDoesNotExistException e) {
		}	
	}

	@Override
	public void close() {
		
	}
	
}
