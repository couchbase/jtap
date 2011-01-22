package com.membase.jtap.ops;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.membase.jtap.exception.FieldDoesNotExistException;
import com.membase.jtap.exporter.Exporter;
import com.membase.jtap.message.Magic;
import com.membase.jtap.message.Opcode;
import com.membase.jtap.message.RequestMessage;
import com.membase.jtap.message.ResponseMessage;

public class CustomStream implements TapStream {
	private static final Logger LOG = LoggerFactory.getLogger(CustomStream.class);

	private long count;
	private RequestMessage message;
	private Exporter exporter;

	public CustomStream(Exporter exporter, String identifier) {
		this.count = 0;
		this.exporter = exporter;
		this.message = new RequestMessage();

		message.setMagic(Magic.PROTOCOL_BINARY_REQ);
		message.setOpcode(Opcode.REQUEST);
		message.setName(identifier);
		
		LOG.info("Custom tap stream created");
	}

	@Override
	public RequestMessage getMessage() {
		return message;
	}

	@Override
	public void receive(ResponseMessage streamMessage) {
		if (streamMessage.getOpcode() != Opcode.NOOP.opcode) {
			String key = streamMessage.getKey();
			try {
				String value = streamMessage.getValue();
				exporter.write(key, value);
			} catch (FieldDoesNotExistException e) {
				exporter.write(key);
			}
			count++;
		}
	}
	
	public void doBackfill(Date date) {
		
	}
	
	public void doDump() {
		
	}
	
	public void specifyVbuckets(int[] vbucketlist) {
		
	}
	
	public void supportAck() {
		
	}
	
	public void keysOnly() {

	}

	@Override
	public long getCount() {
		return count;
	}
}
