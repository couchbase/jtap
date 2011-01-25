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
 * TakeoverVBucketsStream is a template class for starting a basic tap connection to a membase
 * server. It takes a identifier string which can be used to restrat this tap connection if the
 * connection gets closed. This tap stream is used to transfer vBucket ownership from one membase
 * node to another at the end of a dump.
 * 
 * ****Caution****
 * Using this command in the wrong way can cause serious harm to you membase cluster if used
 * incorrectly.
 */
public class TakeoverVBucketsStream implements TapStream {
	private static final Logger LOG = LoggerFactory.getLogger(TakeoverVBucketsStream.class);

	private long count;
	private Exporter exporter;
	private RequestMessage message;

	/**
	 * Creates a default take over vBuckets stream.
	 * @param exporter Specifies how you tap stream data will be exported.
	 * @param identifier Specifies an identifier which can be used to recover a closed tap stream.
	 * @param vbucketlist A list specifying which vBucket to takeover.
	 */
	public TakeoverVBucketsStream(Exporter exporter, String identifier, int[] vbucketlist) {
		this.count = 0;
		this.exporter = exporter;
		this.message = new RequestMessage();

		message.setMagic(Magic.PROTOCOL_BINARY_REQ);
		message.setOpcode(Opcode.REQUEST);
		message.setFlags(Flag.LIST_VBUCKETS);
		message.setFlags(Flag.TAKEOVER_VBUCKETS);
		message.setName(identifier);
		message.setVbucketlist(vbucketlist);
		
		LOG.info("Take over vBucket tap stream created");
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
		if (streamMessage.getOpcode() != Opcode.NOOP.opcode) {
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
}
