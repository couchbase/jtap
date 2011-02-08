package com.membase.jtap.exporter;

import com.membase.jtap.exception.FieldDoesNotExistException;
import com.membase.jtap.message.ResponseMessage;

public class TextExporter implements Exporter {
	
	public TextExporter() {

	}
	
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
