/*
 * Copyright (c) 2010 Membase. All Rights Reserved.
 */

package com.membase.jtap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.membase.jtap.internal.PlainCallbackHandler;
import com.membase.jtap.internal.Response;
import com.membase.jtap.internal.SASLAuthenticator;
import com.membase.jtap.message.BaseMessage;
import com.membase.jtap.message.Magic;
import com.membase.jtap.message.Opcode;
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
	private static final String PROTOCOL = "memcached"; 
	
	private boolean started;
	private String host;
	private String bucket;
	private String password;
	private int port;
	private SocketChannel channel;
	private BlockingQueue<Response> rQueue;
	private BlockingQueue<Response> wQueue;
	private SASLAuthenticator sasl;
	private Thread mbuilder;
	private Thread reader;
	private Thread writer;
	
	public TapStreamClient(String host, int port, String bucket, String password) {
		started = false;
		this.host = host;
		this.port = port;
		this.bucket = bucket;
		this.password = password;
		rQueue = new LinkedBlockingQueue<Response>();
		wQueue = new LinkedBlockingQueue<Response>();
		sasl = null;
	}

	public void start(TapStream tapStream) {
		LOG.info("starting stream client");
		
		if (password != null)
			sasl = new SASLAuthenticator(host, PROTOCOL, bucket, password, wQueue);
		
		channel = connect(tapStream);
		reader = new Thread(new SocketReader(rQueue, channel));
		writer = new Thread(new SocketWriter(wQueue, channel));
		mbuilder = new Thread(new ResponseMessageBuilder(rQueue, tapStream, sasl));
		
		reader.start();
		writer.start();
		mbuilder.start();

		if (password != null)
			sasl.handshake();
		
		LOG.info("initializing tap request");
		RequestMessage message = tapStream.getMessage();
		ByteBuffer bytes;
		message.printMessage();
		bytes = message.getBytes();
		wQueue.add(new Response(bytes, bytes.capacity()));
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
			writer.interrupt();
			reader.interrupt();
			LOG.info("Draining the message queue");
			while (rQueue.size() > 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
			}
			LOG.info("Message Queue Drained");
			mbuilder.interrupt();
			LOG.info("Tap Stream Closed");
			
			
		} else {
			LOG.info("Tap stream not started. Cannot be stopped.");
		}
	}

	private SocketChannel connect(TapStream tapListener) {
		InetSocketAddress socketAddress = new InetSocketAddress(host, port);

		SocketChannel sChannel = null;
		boolean connected = false;
		try {
			sChannel = SocketChannel.open();
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
	private static final Logger LOG = LoggerFactory.getLogger(ResponseMessageBuilder.class);
	private static final int HEADER_LENGTH = 24;
	
	private BlockingQueue<Response> rQueue;
	private TapStream tapStream;
	private ResponseMessage message;
	
	private Response response;
	private ByteBuffer buffer;
	private SASLAuthenticator sasl;
	int bufferLength;
	int position;

	byte[] hBuffer;
	byte[] mBuffer;
	
	public ResponseMessageBuilder(BlockingQueue<Response> rQueue, TapStream tapStream, SASLAuthenticator sasl) {
		this.rQueue = rQueue;
		this.tapStream = tapStream;
		this.message = null;
		this.sasl = sasl;
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
				if (message.getOpcode() == Opcode.SASLLIST.opcode || message.getOpcode() == Opcode.SASLAUTH.opcode)
					sasl.recieve(message);
				else
					tapStream.receive(message);
			}
		} catch (InterruptedException e) {
			LOG.info("ResponseMessageBuilder terminating");
		}
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
	private static final Logger LOG = LoggerFactory.getLogger(SocketReader.class);
	private static final int BUFFER_SIZE = 1024;
	
	BlockingQueue<Response> rQueue;
	SocketChannel channel;
	
	public SocketReader(BlockingQueue<Response> rQueue, SocketChannel channel) {
		this.rQueue = rQueue;
		this.channel = channel;
	}
	
	@Override
	public void run() {
		ByteBuffer rbuf;
		int bytesRead = 0;
		while (bytesRead >= 0) {
			rbuf = ByteBuffer.allocateDirect(BUFFER_SIZE);
			try {
			    rbuf.clear();
			    bytesRead = channel.read(rbuf);
			    if (bytesRead > 0) {
			    	rbuf.flip();
			    	rQueue.add(new Response(rbuf, bytesRead));
			    }
			} catch (IOException e) {
			    System.out.println("Connection Closed");
			    bytesRead = -1;
			}
		}
		LOG.info("SocketReader terminating");
	}
}

class SocketWriter implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(SocketReader.class);
	
	BlockingQueue<Response> wQueue;
	SocketChannel channel;
	
	public SocketWriter(BlockingQueue<Response> wQueue, SocketChannel channel) {
		this.wQueue = wQueue;
		this.channel = channel;
	}
	
	@Override
	public void run() {
		Response res;
		ByteBuffer buffer;
		try {
			while (true) {
				res = wQueue.take();
				buffer = res.getBuffer();
				buffer.position(0);
				channel.write(buffer);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		LOG.info("SocketWriter terminating");
	}
}
