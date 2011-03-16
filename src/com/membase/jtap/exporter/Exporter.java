package com.membase.jtap.exporter;

import com.membase.jtap.message.ResponseMessage;

/**
 * An interface for exporting data from a tap stream.
 */
public interface Exporter {
	/**
	 * Specifies writing tap message information to a given output.
	 * @param message The tap message to write.
	 */
	public void write(ResponseMessage message);
	
	/**
	 * Closes the file descriptor used by this exporter.
	 */
	public void close();
}
