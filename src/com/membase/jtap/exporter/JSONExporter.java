package com.membase.jtap.exporter;

import com.membase.jtap.exception.FieldDoesNotExistException;
import com.membase.jtap.message.ResponseMessage;

public class JSONExporter implements Exporter {
	
	public JSONExporter() {

	}
	
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
