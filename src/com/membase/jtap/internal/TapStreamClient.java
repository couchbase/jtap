/*
 * Copyright (c) 2010 Membase. All Rights Reserved.
 */

package com.membase.jtap.internal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.membase.jtap.message.RequestMessage;
import com.membase.jtap.message.ResponseMessage;
import com.membase.jtap.ops.TapStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TapStreamClient {
	private static final Logger LOG = LoggerFactory.getLogger(TapStreamClient.class);
	
	private boolean started;
	private String host;
	private int port;
	private SocketChannel channel;
	private BlockingQueue<Response> rQueue;
	private Thread mbuilder;
	private Thread reader;
	
	public TapStreamClient(String host, int port) {
		started = false;
		this.host = host;
		this.port = port;
		rQueue = new LinkedBlockingQueue<Response>();
	}

	public void start(TapStream tapStream) {
		LOG.info("starting stream client");

		channel = connect(tapStream);
		
		// Configure SASL Bucket Authentication

		LOG.info("initializing tap request");
		RequestMessage message = tapStream.getMessage();
		message.printMessage();
		handleWrite(message);
		
		reader = new Thread(new SocketReader(rQueue, channel));
		mbuilder = new Thread(new ResponseMessageBuilder(rQueue, tapStream));
		mbuilder.start();
		reader.start();
		started = true;
	}

	public void stop() {
		if (started) {
			LOG.info("stopping stream client");
			
			if (channel.isOpen()) {
				try {
					channel.close();
				} catch (IOException e) {
					LOG.info("Error closing channel");
					e.printStackTrace();
				}
			}
			
			reader.interrupt();
			
			while (rQueue.size() > 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
			}
			mbuilder.interrupt();
			
			
		} else {
			LOG.info("Tap stream not started. Cannot be stopped.");
		}
	}
	
	private int handleWrite(RequestMessage message) {
		int bytesWritten = 0;
		
		ByteBuffer buf = message.getBytes();
		buf.position(0);
		try {
			bytesWritten = channel.write(buf);
		} catch (IOException e) {
			e.printStackTrace();
		} 
		return bytesWritten;
	}

	private SocketChannel connect(TapStream tapListener) {
		InetSocketAddress socketAddress = new InetSocketAddress(host, port);

		SocketChannel sChannel = null;
		boolean connected = false;
		try {
			sChannel = SocketChannel.open();
			//sChannel.configureBlocking(false);
			connected = sChannel.connect(socketAddress);
			
			while (!sChannel.finishConnect())
					Thread.sleep(100);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("Connected: " + connected);
		channel = sChannel;
		LOG.info("connected to {}", socketAddress);
		
		return channel;
	}
}

class ResponseMessageBuilder implements Runnable {
	private static final int HEADER_LENGTH = 24;
	
	private BlockingQueue<Response> rQueue;
	private TapStream tapStream;
	private ResponseMessage message;
	
	private Response response;
	private ByteBuffer buffer;
	int bufferLength;
	int position;

	byte[] hBuffer;
	byte[] mBuffer;
	
	public ResponseMessageBuilder(BlockingQueue<Response> rQueue, TapStream tapStream) {
		this.rQueue = rQueue;
		this.tapStream = tapStream;
		this.message = null;
	}
	
	@Override
	public void run() {
		int bodyLength;
		
		try {
			getNextResponse();
			while(true) {
				parseHeader();
				bodyLength = getTotalBody();
				mBuffer = new byte[HEADER_LENGTH + bodyLength];
				
				for (int i = 0; i < HEADER_LENGTH; i ++)
					mBuffer[i] = hBuffer[i];
				
				for (int i = 0; i < bodyLength; i++, position++) {
					if (position == bufferLength)
						getNextResponse();
					mBuffer[i + HEADER_LENGTH] = buffer.get(position);
				}
				
				message = new ResponseMessage(mBuffer);
				message.printMessage();
				tapStream.receive(message);
			}
		} catch (InterruptedException e) {}
	}
	
	private void parseHeader() throws InterruptedException {
		hBuffer = new byte[HEADER_LENGTH];
		
		for (int i = 0; i < HEADER_LENGTH; i++, position++) {
			if (position == bufferLength)
				getNextResponse();
			hBuffer[i] = buffer.get(position);
		}
	}
	
	private int getTotalBody() {
		//TODO: This is bad coding
		return hBuffer[8] * 16777216 + hBuffer[9] * 65535 + hBuffer[10] * 256 + hBuffer[11];
	}
	
	private void getNextResponse() throws InterruptedException {
		response = rQueue.take();
		buffer = response.getBuffer();
		bufferLength = response.getBufferLength();
		position = 0;
	}
}

class SocketReader implements Runnable {
	private static final int BUFFER_SIZE = 1024;
	
	BlockingQueue<Response> rQueue;
	SocketChannel channel;
	
	public SocketReader(BlockingQueue<Response> rQueue, SocketChannel channel) {
		this.rQueue = rQueue;
		this.channel = channel;
	}
	
	@Override
	public void run() {
		int bytesRead = 0;
		while (bytesRead >= 0) {
			bytesRead = handleReads();
			//System.out.println("Handling Read " + bytesRead + "  bytes read");
		}
	}
	
	private int handleReads() {
		ByteBuffer rbuf = ByteBuffer.allocateDirect(BUFFER_SIZE);
		int bytesRead = -1;
		
		try {
		    rbuf.clear();
		    bytesRead = channel.read(rbuf);
		    if (bytesRead > 0) {
		    	rbuf.flip();
		    	rQueue.add(new Response(rbuf, bytesRead));
		    }
		} catch (IOException e) {
		    System.out.println("Connection Closed");
		    return -1;
		}
		return bytesRead;
	}	
}

class Response {
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
