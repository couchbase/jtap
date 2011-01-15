/*
 * Copyright (c) 2010 Membase. All Rights Reserved.
 */

package com.membase.nodecode.tap.internal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.membase.nodecode.tap.TapStreamConfiguration;
import com.membase.nodecode.tap.message.Magic;
import com.membase.nodecode.tap.message.Response;
import com.membase.nodecode.tap.message.TapStreamMessage;
import com.membase.nodecode.tap.ops.TapStreamConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TapStreamClient {
	
	private static final Logger LOG = LoggerFactory.getLogger(TapStreamClient.class);
	
	private String host;
	private int port;
	private SocketChannel channel;
	private String tapName;
	private TapStreamConfig tapListener;
	private BlockingQueue<Response> rQueue;
	private Thread mbuilder;
	private Thread reader;
	
	public TapStreamClient(String host, int port) {
		this.host = host;
		this.port = port;
		rQueue = new LinkedBlockingQueue<Response>();
	}

	public void start(TapStreamConfig tapConfig) {
		this.tapListener = tapConfig;
		this.tapName = tapConfig.getConfiguration().getTapName();

		LOG.info("starting stream client: {}", tapName);
		TapStreamConfiguration configuration = tapConfig.getConfiguration();

		channel = connect(tapConfig);
		
		// Configure SASL Bucket Authentication

		LOG.info("initializing tap request");
		TapStreamMessage message = tapConfig.getMessage();
		message.printMessage();
		handleWrite(message);
		
		reader = new Thread(new SocketReader(rQueue, channel));
		mbuilder = new Thread(new ResponseMessageBuilder(rQueue, tapConfig));
		mbuilder.start();
		reader.start();
		
		
		
	}

	public void stop() {
		LOG.info("stopping stream client for {}", tapName);
		if (channel.isOpen()) {
			try {
				channel.close();
			} catch (IOException e) {
				LOG.info("Error closing channel");
				e.printStackTrace();
			}
		}
	}
	
	private int handleWrite(TapStreamMessage message) {
		int bytesWritten = 0;
		
		ByteBuffer buf = ByteBuffer.allocateDirect(message.getMessageLength());
		buf.put(message.encode());
		buf.position(0);
		try {
			bytesWritten = channel.write(buf);
		} catch (IOException e) {
			e.printStackTrace();
		} 
		return bytesWritten;
	}

	private SocketChannel connect(TapStreamConfig tapListener) {
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
	private TapStreamConfig tapListener;
	private TapStreamMessage message;
	
	private Response response;
	private ByteBuffer buffer;
	int bufferLength;
	int position;

	byte[] hBuffer;
	byte[] mBuffer;
	
	public ResponseMessageBuilder(BlockingQueue<Response> rQueue, TapStreamConfig tapListener) {
		this.rQueue = rQueue;
		this.tapListener = tapListener;
		this.message = null;
	}
	
	@Override
	public void run() {
		int bodyLength;
		getNextResponse();
		
		while(true) {
			parseHeader();
			bodyLength = getTotalBody();
			mBuffer = new byte[HEADER_LENGTH + bodyLength];
			//System.out.println("Printing Header (mBuffer is " + mBuffer.length + ")");
			for (int i = 0; i < HEADER_LENGTH; i ++) {
				//System.out.printf("%x ", hBuffer[i]);
				mBuffer[i] = hBuffer[i];
			}
			
			for (int i = 0; i < bodyLength; i++, position++) {
				if (position == bufferLength)
					getNextResponse();
				mBuffer[i + HEADER_LENGTH] = buffer.get(position);
			}
			message = new TapStreamMessage();
			message.decode(mBuffer);
			message.printMessageDetails();
		}
	}
	
	private void parseHeader() {
		hBuffer = new byte[HEADER_LENGTH];
		
		for (int i = 0; i < HEADER_LENGTH; i++, position++) {
			if (position == bufferLength)
				getNextResponse();
			hBuffer[i] = buffer.get(position);
		}
	}
	
	private int getTotalBody() {
		return hBuffer[8] * 16777216 + hBuffer[9] * 65535 + hBuffer[10] * 256 + hBuffer[11];
	}
	
	private void getNextResponse() {
		try {
			response = rQueue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Got a response from the queue");
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
		int i = 0;
		while (bytesRead >= 0 && i < 10) {
			i++;
			bytesRead = handleReads();
			System.out.println("Handling Read " + bytesRead + "  bytes read");
		}
		System.out.println("Size: " + rQueue.size());
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
		    // Connection may have been closed
		}
		return bytesRead;
	}
	
}
