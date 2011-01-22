package com.membase.jtap.ops;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.membase.jtap.internal.TapStreamConfiguration;
import com.membase.jtap.message.Flag;
import com.membase.jtap.message.Magic;
import com.membase.jtap.message.Opcode;
import com.membase.jtap.message.RequestMessage;
import com.membase.jtap.message.ResponseMessage;

public class CustomStream implements TapStream {
	private static final Logger LOG = LoggerFactory.getLogger(CustomStream.class);

	private int count;
	private String bucketName;
	private String bucketPassword;
	private RequestMessage message;

	public CustomStream(String bucketName, String bucketPassword, String identifier) {
		this.bucketName = bucketName;
		this.bucketPassword = bucketPassword;
		this.message = new RequestMessage();

		message.setMagic(Magic.PROTOCOL_BINARY_REQ);
		message.setOpcode(Opcode.REQUEST);
		message.setName(identifier);
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
}
