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

/**
 * CustomStream allows for the creation of a tap stream by combining many of the different
 * types tap request together into one single request.<br> 
 * <br>
 * For example, let's say we wanted a tap message that did the following:<br>
 * - We want all future mutations of items in vBucket 0-4<br>
 * - We want server acknowledgments so that we can throttle the amount of tap responses<br>
 * - We only want the key and not the values of each key-value pair<br>
 * <br>
 * We would write the following code:<br>
 * <br>
 * TapStream tapstream = new CustomStream(); // Gives us a basic tap message<br>
 * tapstream.setBackfill(null); // Specifies only future mutations<br>
 * tapstream.specifyVbuckets(new int[] {0, 1, 2, 3, 4}); // Get mutations from vBuckets 0-4<br>
 * tapstream.supportAck(); // Get acknowledgment messages from the server<br>
 * tapstream.keysOnly(); // Only send the keys and not the values<br>
 * <br>
 * Then all thats left is to pass this tap stream to your tap client and start the stream.
 */
public class CustomStream implements TapStream {
	private static final Logger LOG = LoggerFactory.getLogger(CustomStream.class);

	private long count;
	private RequestMessage message;
	private Exporter exporter;

	/**
	 * Creates a default custom stream. The custom tap stream starts out as a tap message header
	 * but can take on greater functionality by calling functions in this class that allow it to
	 * specify more intricate details.
	 * @param exporter Specifies how you tap stream data will be exported.
	 * @param identifier Specifies an identifier which can be used to recover a closed tap stream.
	 */
	public CustomStream(Exporter exporter, String identifier) {
		this.count = 0;
		this.exporter = exporter;
		this.message = new RequestMessage();

		message.setMagic(Magic.PROTOCOL_BINARY_REQ);
		message.setOpcode(Opcode.REQUEST);
		message.setName(identifier);
		
		LOG.info("Custom tap stream created");
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

	/**
	 * Returns the number of messages that this tap stream has handled.
	 */
	@Override
	public long getCount() {
		return count;
	}
}
