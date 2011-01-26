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

/**
 * KeysOnlyStream is a template class for starting a basic tap connection to a membase
 * server. It takes a identifier string which is used to restart connections that are
 * interrupted prematurely. Membase will dump all of the items in a given node and transmit
 * the key name, but not the value of a key-value pair.
 */
public class KeysOnlyStream implements TapStream {
	private static final Logger LOG = LoggerFactory.getLogger(KeysOnlyStream.class);

	private long count;
	private Exporter exporter;
	private RequestMessage message;
	
	/**
	 * Creates a default keys only stream.
	 * @param exporter Specifies how you tap stream data will be exported.
	 * @param identifier Specifies an identifier which can be used to recover a closed tap stream.
	 */
	public KeysOnlyStream(Exporter exporter, String identifier) {
		this.count = 0;
		this.exporter = exporter;
		this.message = new RequestMessage();
		
		message.setMagic(Magic.PROTOCOL_BINARY_REQ);
		message.setOpcode(Opcode.REQUEST);
		message.setFlags(Flag.KEYS_ONLY);
		message.setName(identifier);
		
		LOG.info("Keys only tap stream created");
	}
	
	/**
	 * Returns an object that contains a representation of the tap stream message that initiates the
	 * tap stream.
	 */
	@Override
	public RequestMessage getMessage() {
		return message;
	}

	/**
	 * Specifies how a received tap stream message will interact with the streams exporter.
	 * @param streamMessage The message received from the Membase.
	 */
	@Override
	public void receive(ResponseMessage streamMessage) {
		if (streamMessage.getOpcode() == Opcode.OPAQUE.opcode) {
			// Ignore
		} else if (streamMessage.getOpcode() == Opcode.NOOP.opcode) {
			// Ignore
		} else {
			exporter.write(streamMessage.getKey());
			count++;
		}
	}

	/**
	 * Returns the number of messages that this tap stream has handled.
	 */
	@Override
	public long getCount() {
		return count;
	}

	@Override
	public void cleanup() {
		exporter.close();
	}
}

