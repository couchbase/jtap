package com.membase.jtap.ops;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.membase.jtap.TapStreamConfiguration;
import com.membase.jtap.message.Flag;
import com.membase.jtap.message.Magic;
import com.membase.jtap.message.Opcode;
import com.membase.jtap.message.RequestMessage;
import com.membase.jtap.message.ResponseMessage;

public class BackfillStream implements TapStream {
	private static final Logger LOG = LoggerFactory.getLogger(BackfillStream.class);

	private int count;
	private String bucketName;
	private String bucketPassword;
	private RequestMessage message;

	public BackfillStream(String bucketName, String bucketPassword,
			String identifier) {
		this.bucketName = bucketName;
		this.bucketPassword = bucketPassword;
		this.message = new RequestMessage();

		message.setMagic(Magic.PROTOCOL_BINARY_REQ);
		message.setOpcode(Opcode.REQUEST);
		message.setFlags(Flag.BACKFILL.flag);
		message.setName(identifier);
		message.setBackfill(null);
	}

	@Override
	public TapStreamConfiguration getConfiguration() {
		LOG.debug("sending configuration");
		return new TapStreamConfiguration("testnode", bucketName,
				bucketPassword);
	}

	public RequestMessage getMessage() {
		return message;
	}

	@Override
	public void receive(ResponseMessage streamMessage) {
		if (streamMessage.getOpcode() != Opcode.NOOP.opcode) {
			System.out.println("Key: " + streamMessage.getKey());
			System.out.println("Value: " + streamMessage.getValue());
			count++;
		}
	}
}
