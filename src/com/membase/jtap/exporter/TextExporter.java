package com.membase.jtap.exporter;

public class TextExporter implements Exporter {
	
	public TextExporter() {

	}
	
	@Override
	public void write(String key) {
		System.out.println("Key: " + key);	
	}

	@Override
	public void write(String key, String value) {
		System.out.println("Key: " + key);
		System.out.println("Value: " + value);
	}

	@Override
	public void close() {
		
	}
	
}
