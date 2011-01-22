/*
 * Copyright (c) 2010 Membase. All Rights Reserved.
 */

package com.membase.jtap.ops;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.membase.jtap.internal.TapStreamConfiguration;
import com.membase.jtap.message.Flag;
import com.membase.jtap.message.Magic;
import com.membase.jtap.message.Opcode;
import com.membase.jtap.message.RequestMessage;
import com.membase.jtap.message.ResponseMessage;

/**
 *
 */
public class DumpStream implements TapStream {
	private static final Logger LOG = LoggerFactory.getLogger(DumpStream.class);

	private int count;
	private String bucketName;
	private String bucketPassword;
	private RequestMessage message;
	
	public DumpStream(String bucketName, String bucketPassword, String identifier) {
		this.bucketName = bucketName;
		this.bucketPassword = bucketPassword;
		this.message = new RequestMessage();
		
		message.setMagic(Magic.PROTOCOL_BINARY_REQ);
		message.setOpcode(Opcode.REQUEST);
		message.setFlags(Flag.DUMP.flag);
		message.setName(identifier);
	}

	@Override
	public TapStreamConfiguration getConfiguration() {
		LOG.debug("sending configuration");
		return new TapStreamConfiguration("testnode", bucketName, bucketPassword);
	}
	
	public RequestMessage getMessage() {
		return message;
	}

	@Override
	public void receive(ResponseMessage streamMessage) {
		System.out.println("Key: " + streamMessage.getKey());
		System.out.println("Value: " + streamMessage.getValue());
		count++;
		System.out.println("Count: " + count);
	}
}
