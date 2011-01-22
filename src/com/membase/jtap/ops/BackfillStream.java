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
 * server. It takes a identifier string and a date that specifies which items to send.
 * Membase will send all key-value pairs with a timestamp set after the specified date.
 * If the date is in the future membase will send all future key-value mutations.
 */
public class BackfillStream implements TapStream {
	private static final Logger LOG = LoggerFactory.getLogger(BackfillStream.class);

	private long count;
	private Exporter exporter;
	private RequestMessage message;

	public BackfillStream(Exporter exporter, String identifier, Date date) {
		this.count = 0;
		this.exporter = exporter;
		this.message = new RequestMessage();

		message.setMagic(Magic.PROTOCOL_BINARY_REQ);
		message.setOpcode(Opcode.REQUEST);
		message.setFlags(Flag.BACKFILL.flag);
		message.setName(identifier);
		message.setBackfill(date);
		
		LOG.info("Backfill tap stream created");
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
	
	@Override
	public long getCount() {
		return count;
	}
}
