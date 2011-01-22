package com.membase.jtap.ops;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.membase.jtap.exporter.Exporter;
import com.membase.jtap.message.Flag;
import com.membase.jtap.message.Magic;
import com.membase.jtap.message.Opcode;
import com.membase.jtap.message.RequestMessage;
import com.membase.jtap.message.ResponseMessage;

public class TakeoverVBucketsStream implements TapStream {
	private static final Logger LOG = LoggerFactory.getLogger(TakeoverVBucketsStream.class);

	private long count;
	private Exporter exporter;
	private RequestMessage message;

	public TakeoverVBucketsStream(Exporter exporter, String identifier, int[] vbucketlist) {
		this.count = 0;
		this.exporter = exporter;
		this.message = new RequestMessage();

		message.setMagic(Magic.PROTOCOL_BINARY_REQ);
		message.setOpcode(Opcode.REQUEST);
		message.setFlags(Flag.LIST_VBUCKETS.flag + Flag.TAKEOVER_VBUCKETS.flag);
		message.setName(identifier);
		message.setVbucketlist(vbucketlist);
		
		LOG.info("Take over vBucket tap stream created");
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
