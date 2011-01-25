package com.membase.jtap.internal;

import java.nio.ByteBuffer;

/**
 * Holds ByteBuffers that have been read from the tap stream
 */
public class Response {
	
	/**
	 * The bytes that were read from the tap stream
	 */
	private ByteBuffer buffer;
	
	/**
	 * The length of the buffer holding the bytes red from the tap stream
	 */
	private int bufferLength;
	
	/**
	 * Creates an object that holds bytes read from the tap stream
	 * @param buffer
	 * @param bufferLength
	 */
	public Response(ByteBuffer buffer, int bufferLength) {
		this.buffer = buffer;
		this.bufferLength = bufferLength;
	}
	
	/**
	 * Gets the bytes read from the tap stream
	 * @return The bytes read from the tap stream
	 */
	public ByteBuffer getBuffer() {
		return buffer;
	}
	
	/**
	 * Gets the number of bytes read from the tap stream
	 * @return The length of the buffer
	 */
	public int getBufferLength() {
		return bufferLength;
	}
}
