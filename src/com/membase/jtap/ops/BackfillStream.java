package com.membase.jtap.ops;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.membase.jtap.exporter.Exporter;
import com.membase.jtap.message.Flag;
import com.membase.jtap.message.Magic;
import com.membase.jtap.message.Opcode;
import com.membase.jtap.message.RequestMessage;
import com.membase.jtap.message.ResponseMessage;

/**
 * BackfillStream is a template class for starting a basic tap connection to a membase
 * server. It takes a identifier string and a date that specifies which items the 
 * Membase server should send. Membase will send all key-value pairs with a timestamp 
 * set after the specified date. If the date is in the future membase will send all 
 * future key-value mutations.
 */
public class BackfillStream implements TapStream {
	private static final Logger LOG = LoggerFactory.getLogger(BackfillStream.class);

	private long count;
	private Exporter exporter;
	private RequestMessage message;

	/**
	 * Creates a default backfill stream.
	 * @param exporter Specifies how you tap stream data will be exported.
	 * @param identifier Specifies an identifier which can be used to recover a closed tap stream.
	 * @param date Specifies a date for when to start streaming items. If this date is set to any
	 * time in the future then the tap stream will immediately begin streaming any new tap mutations.
	 */
	public BackfillStream(Exporter exporter, String identifier, Date date) {
		this.count = 0;
		this.exporter = exporter;
		this.message = new RequestMessage();

		message.setMagic(Magic.PROTOCOL_BINARY_REQ);
		message.setOpcode(Opcode.REQUEST);
		message.setFlags(Flag.BACKFILL);
		message.setName(identifier);
		message.setBackfill(date);
		
		LOG.info("Backfill tap stream created");
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
			exporter.write(streamMessage.getKey(), streamMessage.getValue());
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
