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
public class KeysOnlyStreamConfig implements TapStreamConfig {
	private static final Logger LOG = LoggerFactory
			.getLogger(DumpStreamConfig.class);

	private int count;
	private String bucketName;
	private String bucketPassword;
	private String identifier;
	private TapStreamMessage message;
	
	public KeysOnlyStreamConfig(String bucketName, String bucketPassword, String identifier) {
		this.bucketName = bucketName;
		this.bucketPassword = bucketPassword;
		this.identifier = identifier;
		this.message = new TapStreamMessage();
		
		Flag[] flags = new Flag[1];
		flags[0] = Flag.KEYS_ONLY;
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
	
	public TapStreamMessage getMessage() {
		return message;
	}

	@Override
	public void receive(TapStreamMessage streamMessage) {
		count++;
	}
}

