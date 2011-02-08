package com.membase.jtap.exporter;

import com.membase.jtap.message.ResponseMessage;

public interface Exporter {
	public void write(ResponseMessage message);
	public void close();
}
