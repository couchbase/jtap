/*
 * Copyright (c) 2010 Membase. All Rights Reserved.
 */

package com.membase.jtap.ops;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.membase.jtap.exporter.Exporter;
import com.membase.jtap.message.Flag;
import com.membase.jtap.message.Magic;
import com.membase.jtap.message.Opcode;
import com.membase.jtap.message.RequestMessage;
import com.membase.jtap.message.ResponseMessage;

public class KeysOnlyStream implements TapStream {
	private static final Logger LOG = LoggerFactory.getLogger(KeysOnlyStream.class);

	private long count;
	private Exporter exporter;
	private RequestMessage message;
	
	public KeysOnlyStream(Exporter exporter, String identifier) {
		this.exporter = exporter;
		this.message = new RequestMessage();
		
		message.setMagic(Magic.PROTOCOL_BINARY_REQ);
		message.setOpcode(Opcode.REQUEST);
		message.setFlags(Flag.KEYS_ONLY.flag);
		message.setName(identifier);
		
		LOG.info("Keys only tap stream created");
	}
	
	@Override
	public RequestMessage getMessage() {
		return message;
	}

	@Override
	public void receive(ResponseMessage streamMessage) {
		if (streamMessage.getOpcode() != Opcode.NOOP.opcode) {
			exporter.write(streamMessage.getKey());
		}
		count++;
	}
}

