package com.membase.jtap.internal;

import java.nio.ByteBuffer;

public class Response {
	private ByteBuffer buffer;
	private int bufferLength;
	
	public Response(ByteBuffer buffer, int bufferLength) {
		this.buffer = buffer;
		this.bufferLength = bufferLength;
	}
	
	public ByteBuffer getBuffer() {
		return buffer;
	}
	
	public int getBufferLength() {
		return bufferLength;
	}
}
