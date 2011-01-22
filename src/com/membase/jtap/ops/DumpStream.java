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

public class DumpStream implements TapStream {
	private static final Logger LOG = LoggerFactory.getLogger(DumpStream.class);

	private long count;
	private RequestMessage message;
	private Exporter exporter;
	
	public DumpStream(Exporter exporter, String identifier) {
		this.exporter = exporter;
		this.message = new RequestMessage();
		
		message.setMagic(Magic.PROTOCOL_BINARY_REQ);
		message.setOpcode(Opcode.REQUEST);
		message.setFlags(Flag.DUMP.flag);
		message.setName(identifier);
		
		LOG.info("Dump tap stream created");
	}
	
	@Override
	public RequestMessage getMessage() {
		return message;
	}

	@Override
	public void receive(ResponseMessage streamMessage) {
		if (streamMessage.getOpcode() != Opcode.NOOP.opcode) {
			exporter.write(streamMessage.getKey(), streamMessage.getValue());
			count++;
		}
	}
}
