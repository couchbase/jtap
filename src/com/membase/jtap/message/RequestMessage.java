package com.membase.jtap.message;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.membase.jtap.TapStreamClient;
import com.membase.jtap.internal.Util;

/**
 * A tap request message that is used to start tap streams, perform sasl authentication, and
 * maintain the health of tap streams.
 */
public class RequestMessage extends HeaderMessage{
	private static final Logger LOG = LoggerFactory.getLogger(RequestMessage.class);
	
	private static final int FLAGS_FIELD_LENGTH = 4;
	private static final int BACKFILL_DATE_FIELD_LENGTH = 8;
	private static final int VBUCKET_LIST_FIELD_LENGTH = 2;
	
	private byte[] name;
	private byte[] flags;
	private byte[] backfilldate;
	private byte[] vbucketlist;
	private byte[] value;
	
	/**
	 * Create a tap request message. These messages are used to start tap streams.
	 */
	public RequestMessage() {
		flags = new byte[0];
		name = new byte[0];
		backfilldate = new byte[0];
		vbucketlist = new byte[0];
		value = new byte[0];
	}
	
	/**
	 * Sets the flags for the tap stream. These flags decide what kind of tap stream 
	 * will be received.
	 * @param f The flags to use for this tap stream.
	 */
	public void setFlags(Flag f) {
		if (flags.length != FLAGS_FIELD_LENGTH)
			flags = new byte[FLAGS_FIELD_LENGTH];
		if (!f.hasFlag(getFlags())) {
			Util.valueToField(flags, 0, FLAGS_FIELD_LENGTH, (long) f.flag);
			encode();
		}
	}
	
	/**
	 * Stream all keys inserted into the server after a given date.
	 * @param date - The date to stream keys from. Null to stream all keys.
	 */
	public void setBackfill(Date date) {
		backfilldate = new byte[BACKFILL_DATE_FIELD_LENGTH];
		if (date == null) {
			for (int i = 0; i < 8; i++) 
				backfilldate[i] = -1;
		} else {
			Util.valueToField(backfilldate, 0, BACKFILL_DATE_FIELD_LENGTH, date.getTime());
		} 
		encode();
	}
	
	/**
	 * Returns the flags for this message.
	 * @return An int value of flags set for this tap message.
	 */
	public int getFlags() {
		if (flags.length != FLAGS_FIELD_LENGTH)
			return 0;
		return (int) Util.fieldToValue(flags, 0, FLAGS_FIELD_LENGTH);
	}
	
	/**
	 * Sets a list of vbuckets to stream keys from.
	 * @param vbs - A list of vbuckets.
	 */
	public void setVbucketlist(int[] vbs) {
		byte[] vblist = new byte[(vbs.length + 1) * VBUCKET_LIST_FIELD_LENGTH];
		for (int i = 0; i < vbs.length + 1; i++) {
			if (i == 0)
				Util.valueToField(vblist, 0, VBUCKET_LIST_FIELD_LENGTH, (long) vbs.length);
			else if (vbs[i-1] < TapStreamClient.NUM_VBUCKETS && vbs[i-1] >= 0)
				Util.valueToField(vblist, (i * VBUCKET_LIST_FIELD_LENGTH), VBUCKET_LIST_FIELD_LENGTH, (long) vbs[i-1]);
			else
				LOG.info("vBucket ignored " + vbs[i-1] + "is not a valid vBucket number");
		}
		vbucketlist = vblist;
		encode();
	}
	
	/**
	 * Sets a name for this tap stream. If the tap strem fails this name can be used to try to restart
	 * the tap stream from where it last left off.
	 * @param s The name for the tap stream.
	 */
	public void setName(String s) {
		// TODO: This needs to throw an exception
		long len = s.length();
		if (len >= (int)Math.pow(256, (double) (KEY_LENGTH_FIELD_LENGTH)))
			System.out.println("Name too big");
		name = s.getBytes();
		setKeylength(len);
		encode();
	}
	
	/**
	 * Used to set an extra value when doing sasl authentication.
	 * @param s The value to be set.
	 */
	public void setValue(String s) {
		value = s.getBytes();
		encode();
	}
	
	/**
	 * Encodes the message into binary.
	 */
	private void encode() {
		byte[] buffer = new byte[HEADER_LENGTH + name.length + flags.length + vbucketlist.length + backfilldate.length + value.length];
		
		int totalbody = 0; // Begin recording total body
		int extralength = 0; // Begin recording extra length
		
		for (int i = 0; i < flags.length; totalbody++, extralength++, i++)
			buffer[HEADER_LENGTH + totalbody] = flags[i];
		setExtralength(extralength); // Stop recording extra length
		
		for (int i = 0; i < name.length; totalbody++, i++)
			buffer[HEADER_LENGTH + totalbody] = name[i];
		
		if (Flag.BACKFILL.hasFlag(getFlags())) {
			for (int i = 0; i < backfilldate.length; totalbody++, i++)
				buffer[HEADER_LENGTH + totalbody] = backfilldate[i];
		}
		
		if (Flag.LIST_VBUCKETS.hasFlag(getFlags())) {
			for (int i = 0; i < vbucketlist.length; totalbody++, i++)
				buffer[HEADER_LENGTH + totalbody] = vbucketlist[i];
		}
		
		for (int i = 0; i < value.length; totalbody++, i++)
			buffer[HEADER_LENGTH + totalbody] = value[i];
		setTotalbody(totalbody); // Stop recording total body
		
		// Do this last because we had to figure out what total body and extra length were
		for (int i = 0; i < HEADER_LENGTH; i++)
			buffer[i] = mbytes[i];
		
		mbytes = buffer;
	}
}
