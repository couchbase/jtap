package com.membase.jtap.exporter;

public interface Exporter {
	public void write(String key);
	public void write(String key, String value);
	public void close();
}
