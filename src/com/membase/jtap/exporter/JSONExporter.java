package com.membase.jtap.exporter;

public class JSONExporter implements Exporter {
	
	public JSONExporter() {

	}
	
	@Override
	public void write(String key) {
		System.out.println("{" + key + "}");	
	}

	@Override
	public void write(String key, String value) {
		System.out.println("{" + key + ":" + value + "}");
	}

	@Override
	public void close() {
		
	}
}
