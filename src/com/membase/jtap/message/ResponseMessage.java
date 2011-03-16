package com.membase.jtap.message;

import com.membase.jtap.exception.FieldDoesNotExistException;
import com.membase.jtap.internal.Util;

/**
 * A representation of a tap stream message sent from a tap stream server.
 */
public class ResponseMessage extends HeaderMessage{
	// Offsets are given from the end of the header
	private static final int ENGINE_PRIVATE_OFFSET = 0;
	private static final int ENGINE_PRIVATE_FIELD_LENGTH = 2;
	private static final int FLAGS_OFFSET = 2;
	private static final int FLAGS_FIELD_LENGTH = 2;
	private static final int TTL_OFFSET = 3;
	private static final int TTL_FIELD_LENGTH = 1;
	private static final int RESERVED1_OFFSET = 4;
	private static final int RESERVED1_FIELD_LENGTH = 1;
	private static final int RESERVED2_OFFSET = 5;
	private static final int RESERVED2_FIELD_LENGTH = 1;
	private static final int RESERVED3_OFFSET = 6;
	private static final int RESERVED3_FIELD_LENGTH = 1;
	private static final int ITEM_FLAGS_OFFSET = 7;
	private static final int ITEM_FLAGS_FIELD_LENGTH = 4;
	private static final int ITEM_EXPIRY_OFFSET = 11;
	private static final int ITEM_EXPIRY_FIELD_LENGTH = 5;
	
	/**
	 * Creates a ResponseMessage from binary data.
	 * @param buffer The binary data sent from the tap stream server.
	 */
	public ResponseMessage(byte[] buffer) {
		mbytes = buffer;
	}
	
	/**
	 * Gets the value of the engine private field if the field exists in the message.
	 * @return The engine private data.
	 */
	public long getEnginePrivate() {
		if (ENGINE_PRIVATE_OFFSET + ENGINE_PRIVATE_FIELD_LENGTH > getExtralength())
			throw new FieldDoesNotExistException("Engine Private field is not defined in this message");
		int offset = HEADER_LENGTH + ENGINE_PRIVATE_OFFSET;
		return Util.fieldToValue(mbytes, offset, ENGINE_PRIVATE_FIELD_LENGTH);
	}
	
	/**
	 * Gets the value of the flags field if the field exists in the message.
	 * @return The flags data.
	 */
	public int getFlags() {
		if (FLAGS_OFFSET + FLAGS_FIELD_LENGTH > getExtralength())
			throw new FieldDoesNotExistException("Flags field is not defined in this message");
		int offset = HEADER_LENGTH + FLAGS_OFFSET;
		return (int) Util.fieldToValue(mbytes, offset, FLAGS_FIELD_LENGTH);
	}
	
	/**
	 * Gets the value of the ttl field if the field exists in the message.
	 * @return The ttl data.
	 */
	public int getTTL() {
		if (TTL_OFFSET + TTL_FIELD_LENGTH > getExtralength())
			throw new FieldDoesNotExistException("TTL field is not defined in this message");
		int offset = HEADER_LENGTH + TTL_OFFSET;
		return (int) Util.fieldToValue(mbytes, offset, TTL_FIELD_LENGTH); 
	}
	
	/**
	 * Gets the value of the reserved1 field if the field exists in the message.
	 * @return The reserved1 data.
	 */
	public int getReserved1() {
		if (RESERVED1_OFFSET + RESERVED1_FIELD_LENGTH > getExtralength())
			throw new FieldDoesNotExistException("First Reserved field is not defined in this message");
		int offset = HEADER_LENGTH + RESERVED1_OFFSET;
		return (int) Util.fieldToValue(mbytes, offset, RESERVED1_FIELD_LENGTH);
	}
	
	/**
	 * Gets the value of the reserved2 field if the field exists in the message.
	 * @return The reserved2 data.
	 */
	public int getReserved2() {
		if (RESERVED2_OFFSET + RESERVED2_FIELD_LENGTH > getExtralength())
			throw new FieldDoesNotExistException("Second Reserved field is not defined in this message");
		int offset = HEADER_LENGTH + RESERVED2_OFFSET;
		return (int) Util.fieldToValue(mbytes, offset, RESERVED2_FIELD_LENGTH);
	}
	
	/**
	 * Gets the value of the reserved3 field if the field exists in the message.
	 * @return The reserved3 data.
	 */
	public int getReserved3() {
		if (RESERVED3_OFFSET + RESERVED3_FIELD_LENGTH > getExtralength())
			throw new FieldDoesNotExistException("Third Reserved field is not defined in this message");
		int offset = HEADER_LENGTH + RESERVED3_OFFSET;
		return (int) Util.fieldToValue(mbytes, offset, RESERVED3_FIELD_LENGTH);
	}
	
	/**
	 * Gets the value of the items flag field if the field exists in the message.
	 * @return The items flag data.
	 */
	public int getItemFlags() {
		if (ITEM_FLAGS_OFFSET + ITEM_FLAGS_FIELD_LENGTH > getExtralength())
			throw new FieldDoesNotExistException("Item Flags field is not defined in this message");
		int offset = HEADER_LENGTH + ITEM_FLAGS_OFFSET;
		return (int) Util.fieldToValue(mbytes, offset, ITEM_FLAGS_FIELD_LENGTH);
	}
	
	/**
	 * Gets the value of the item expiry field if the field exists in the message.
	 * @return The item expiry data.
	 */
	public long getItemExpiry() {
		if (ITEM_EXPIRY_OFFSET + ITEM_EXPIRY_FIELD_LENGTH > getExtralength())
			throw new FieldDoesNotExistException("Item Flags field is not defined in this message");
		int offset = HEADER_LENGTH + ITEM_EXPIRY_OFFSET;
		return Util.fieldToValue(mbytes, offset, ITEM_EXPIRY_FIELD_LENGTH);
	}
	
	/**
	 * Gets the value of the key field if the field exists in the message.
	 * @return The key data.
	 */
	public String getKey() {
		if (getExtralength() >= getTotalbody())
			throw new FieldDoesNotExistException("Key field is not defined in this message");
		int offset = (int) (HEADER_LENGTH + getExtralength());
		return new String(mbytes, offset, getKeylength());
	}
	
	/**
	 * Gets the value of the value field if the field exists in the message.
	 * @return The value data.
	 */
	public String getValue() {
		if (getExtralength() + getKeylength() >= getTotalbody())
			throw new FieldDoesNotExistException("Value field is not defined in this message");
		int offset = (int) (HEADER_LENGTH + getExtralength() + getKeylength());
		int length = (int) (getTotalbody() - getKeylength() - getExtralength());
		return new String(mbytes, offset, length);
	}
	
	/**
	 * Prints out the details of a response message for debugging purposes.
	 */
	public void printMessageDetails() {
		super.printMessageDetails();
		System.out.printf("Engine Private: %d\n", getEnginePrivate());
		System.out.printf("Flags: %d\n", getFlags());
		System.out.printf("TTL: %d\n", getTTL());
		System.out.printf("Reserved 1: %d\n", getReserved1());
		System.out.printf("Reserved 2: %d\n", getReserved2());
		System.out.printf("Reserved 3: %d\n", getReserved3());
		try {
			System.out.printf("Item Flags: %d\n", getItemFlags());
			System.out.printf("Item Expiry: %d\n", getItemExpiry());
			System.out.printf("Key: %s\n", getKey());
			System.out.printf("Value: %s\n", getValue());
		} catch (FieldDoesNotExistException e) {
			System.out.println(e.getMessage());
		}
		System.out.println("---------------------------");
	}
}
