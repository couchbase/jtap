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
 * ListVBucketStream is a template class for starting a basic tap connection to a membase
 * server. It takes a identifier string and a vBucket list that specifies which buckets
 * to stream mutations from.
 */
public class ListVBucketsStream implements TapStream{
	private static final Logger LOG = LoggerFactory.getLogger(ListVBucketsStream.class);

	private long count;
	private Exporter exporter;
	private RequestMessage message;

	/**
	 * Creates a default list vBuckets stream.
	 * @param exporter Specifies how you tap stream data will be exported.
	 * @param identifier Specifies an identifier which can be used to recover a closed tap stream.
	 * @param vbucketlist A list specifying which vBucket to get mutations for.
	 */
	public ListVBucketsStream(Exporter exporter, String identifier, int[] vbucketlist) {
		this.count = 0;
		this.exporter = exporter;
		this.message = new RequestMessage();

		message.setMagic(Magic.PROTOCOL_BINARY_REQ);
		message.setOpcode(Opcode.REQUEST);
		message.setFlags(Flag.LIST_VBUCKETS);
		message.setName(identifier);
		message.setVbucketlist(vbucketlist);
		
		LOG.info("List vBucket tap stream created");
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
		// TODO: Should I be synchronized
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
