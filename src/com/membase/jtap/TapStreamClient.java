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
import java.util.concurrent.TimeUnit;

import com.membase.jtap.internal.Response;
import com.membase.jtap.internal.SASLAuthenticator;
import com.membase.jtap.internal.Util;
import com.membase.jtap.message.HeaderMessage;
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
	public static final int NUM_VBUCKETS = 1024;
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
	
	/**
	 * Creates a tap stream client.
	 * @param host The server to create a tap stream for.
	 * @param port The port number for the server (Typically 11210).
	 * @param bucket The name of the bucket to create a tap stream for.
	 * @param password The password for the bucket, null is there is none.
	 */
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

	/**
	 * Starts a tap stream
	 * @param tapStream The tap stream configuration
	 */
	public void start(TapStream tapStream) {
		LOG.info("Starting tap stream");
		
		if (password != null)
			sasl = new SASLAuthenticator(host, PROTOCOL, bucket, password, wQueue);
		
		channel = connect(tapStream);
		reader = new Thread(new SocketReader(rQueue, channel));
		writer = new Thread(new SocketWriter(wQueue, channel));
		mbuilder = new Thread(new MessageBuilder(reader, rQueue, tapStream, sasl));
		
		reader.start();
		writer.start();
		mbuilder.start();

		if (password != null)
			sasl.handshake();
		
		LOG.info("initializing tap request");
		RequestMessage message = tapStream.getMessage();
		ByteBuffer bytes;
		bytes = message.getBytes();
		wQueue.add(new Response(bytes, bytes.capacity()));
		started = true;
		LOG.info("Tap stream started");
	}

	/**
	 * Stops a tap stream.
	 */
	public void stop() {
		if (started) {
			LOG.info("Stopping Tap Stream");
			if (channel.isOpen()) {
				try {
					channel.close();
				} catch (IOException e) {
					LOG.error("Error closing channel");
					e.printStackTrace();
				}
			}
			LOG.info("Tap Stream Closed");
		} else {
			LOG.info("Tap stream not started. Cannot be stopped.");
		}
	}
	
	/**
	 * Checks to see if there is a tap stream currently running.
	 * @return true if the tap stream is running, false otherwise.
	 */
	public boolean isRunning() {
		return channel.isOpen();
	}

	private SocketChannel connect(TapStream tapListener) {
		InetSocketAddress socketAddress = new InetSocketAddress(host, port);

		SocketChannel sChannel = null;
		try {
			sChannel = SocketChannel.open();
			sChannel.connect(socketAddress);
			
			while (!sChannel.finishConnect())
					Thread.sleep(100);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		channel = sChannel;
		LOG.info("connected to ", socketAddress);
		
		return channel;
	}
}

/**
 * Builds tap messages given a stream of binary
 */
class MessageBuilder implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(MessageBuilder.class);
	
	private BlockingQueue<Response> rQueue;
	private TapStream tapStream;
	private Thread reader;
	private SASLAuthenticator sasl;
	
	private ByteBuffer bbuf;
	int blen;
	int bpos;

	byte[] hbuf;
	int mpos;
	
	int bodylen;
	byte[] mbuf;
	
	public MessageBuilder(Thread reader, BlockingQueue<Response> rQueue, TapStream tapStream, SASLAuthenticator sasl) {
		this.reader = reader;
		this.rQueue = rQueue;
		this.tapStream = tapStream;
		this.sasl = sasl;
	}
	
	@Override
	public void run() {
		bodylen = 0;
		mpos = 0;
		boolean headerparsed = false;
		
		while (reader.getState() != Thread.State.TERMINATED || rQueue.size() > 0) {
			getNextResponse();
			if (bbuf != null) {
				while (bpos < blen) {
					if (!headerparsed)
						headerparsed = parseHeader();
					
					if (headerparsed) {
						for (; 0 < bodylen && bpos < blen; mpos++, bpos++, bodylen--)
							mbuf[mpos] = bbuf.get(bpos);
						
						if (bodylen == 0) {
							ResponseMessage message = new ResponseMessage(mbuf);
							if (message.getOpcode() == Opcode.SASLLIST.opcode || message.getOpcode() == Opcode.SASLAUTH.opcode) {
								sasl.recieve(message);
							} else {
								tapStream.receive(message);
							}
							headerparsed = false;
							mpos = 0;
						}
					}
				}
			}
		}
		tapStream.cleanup();
		LOG.info("Read " + tapStream.getCount() + " tap stream messages from Membase node");
		LOG.info("MessageBuilder terminating");
	}
	
	private boolean parseHeader() {
		if (mpos == 0)
			hbuf = new byte[HeaderMessage.HEADER_LENGTH];
		
		for (; mpos < HeaderMessage.HEADER_LENGTH && bpos < blen; bpos++, mpos++)
			hbuf[mpos] = bbuf.get(bpos);
		
		if (bpos < blen) {
			bodylen = (int) Util.fieldToValue(hbuf, HeaderMessage.TOTAL_BODY_INDEX, HeaderMessage.TOTAL_BODY_FIELD_LENGTH);
			mbuf = new byte[HeaderMessage.HEADER_LENGTH + bodylen];
			
			for (int i = 0; i < HeaderMessage.HEADER_LENGTH; i++)
				mbuf[i] = hbuf[i];
			
			return true;
		}
		return false;
	}
	
	private void getNextResponse() {
		try {
			Response response = rQueue.poll(1000, TimeUnit.MILLISECONDS);
			if (response == null) {
				bbuf = null;
				blen = 0;
			} else {
				bbuf = response.getBuffer();
				blen = response.getBufferLength();
			}
		} catch (InterruptedException e) {
			bbuf = null;
			blen = 0;
		}
		bpos = 0;
	}
}

/**
 * Synchronously reads binary data from a socket.
 */
class SocketReader implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(SocketReader.class);
	private static final int MAX_RQUEUE_SIZE = 20480;
	private static final int BUFFER_SIZE = 256;
	
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
			if (rQueue.size() > MAX_RQUEUE_SIZE)
				backoff();
			rbuf = ByteBuffer.allocateDirect(BUFFER_SIZE);
			try {
			    rbuf.clear();
			    bytesRead = channel.read(rbuf);
			    if (bytesRead > 0) {
			    	rbuf.flip();
			    	rQueue.put(new Response(rbuf, bytesRead));
			    }
			} catch (IOException e) {
			    LOG.info("Connection Closed");
			    bytesRead = -1;
			} catch (InterruptedException e) {
				LOG.info("Interrupted adding buffer to queue from SocketReader");
				e.printStackTrace();
			}
		}
		if (channel.isOpen()) {
			try {
				channel.close();
			} catch (IOException e) {
				LOG.info("Error closing connection to server");
				e.printStackTrace();
			}
		}
		LOG.info("SocketReader terminating");
	}
	
	private void backoff() {
		try {
			while (rQueue.size() > MAX_RQUEUE_SIZE) {
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {
			LOG.info("SocketReader backoff stopped");
		}
	}
}

/**
 * Writes message to the server to initiate and maintain tap streams
 */
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
			while (channel.isOpen()) {
				res = wQueue.poll(1000, TimeUnit.MILLISECONDS);
				if (res != null) {
					buffer = res.getBuffer();
					buffer.position(0);
					channel.write(buffer);
				}
			}
		} catch (InterruptedException e) {
		} catch (IOException e) {
			LOG.error("Error communicating with Membase server");
			e.printStackTrace();
		}
		LOG.info("SocketWriter terminating");
	}
}
