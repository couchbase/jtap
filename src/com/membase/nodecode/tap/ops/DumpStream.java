/*
 * Copyright (c) 2010 Membase. All Rights Reserved.
 */

package com.membase.nodecode.tap.ops;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.membase.nodecode.tap.TapStreamConfiguration;
import com.membase.nodecode.tap.TapStreamType;
import com.membase.nodecode.tap.message.Flag;
import com.membase.nodecode.tap.message.Magic;
import com.membase.nodecode.tap.message.Opcode;
import com.membase.nodecode.tap.message.TapStreamMessage;

/**
 *
 */
public class DumpStream implements TapStream {
	private static final Logger LOG = LoggerFactory
			.getLogger(DumpStream.class);

	private int count;
	private boolean cleanup;
	private String bucketName;
	private String bucketPassword;
	private TapStreamMessage message;

	public DumpStream(String bucketName, String bucketPassword, String identifier) {
		this.bucketName = bucketName;
		this.bucketPassword = bucketPassword;
		this.cleanup = false;
		
		Flag[] flags = new Flag[1];
		flags[0] = Flag.DUMP;
		message = new TapStreamMessage();
		message.setMagic(Magic.PROTOCOL_BINARY_REQ);
		message.setOpcode(Opcode.REQUEST);
		message.setFlags(flags);
		message.setName(identifier);
		
		message.setTotalbody(9);
		message.setExtralength(4);
	}

	@Override
	public TapStreamConfiguration getConfiguration() {
		LOG.debug("sending configuration");
		return new TapStreamConfiguration("testnode", bucketName, bucketPassword, TapStreamType.DUMP);
	}

	@Override
	public void prepare() {
		LOG.debug("prepare called");
	}
	
	public TapStreamMessage getMessage() {
		return message;
	}

	@Override
	public void receive(TapStreamMessage streamMessage) {
		// LOG.debug( "received: " + Integer.toHexString( streamMessage.op ) );
		count++;
	}

	@Override
	public void cleanup() {
		LOG.debug("cleanup called");
		cleanup = true;
	}
}
