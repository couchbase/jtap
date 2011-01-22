/*
 * Copyright (c) 2010 Membase. All Rights Reserved.
 */

package com.membase.jtap.message;

import java.nio.ByteBuffer;

/**
 *
 */
public class BaseMessage {
	protected static final int MAGIC_INDEX = 0;
	protected static final int MAGIC_FIELD_LENGTH = 1;
	protected static final int OPCODE_INDEX = 1;
	protected static final int OPCODE_FIELD_LENGTH = 1;
	protected static final int KEY_LENGTH_INDEX = 2;
	protected static final int KEY_LENGTH_FIELD_LENGTH = 2;
	protected static final int EXTRA_LENGTH_INDEX = 4;
	protected static final int EXTRA_LENGTH_FIELD_LENGTH = 1;
	protected static final int DATA_TYPE_INDEX = 5;
	protected static final int DATA_TYPE_FIELD_LENGTH = 1;
	protected static final int VBUCKET_INDEX = 6;
	protected static final int VBUCKET_FIELD_LENGTH = 2;
	protected static final int TOTAL_BODY_INDEX = 8;
	protected static final int TOTAL_BODY_FIELD_LENGTH = 4;
	protected static final int OPAQUE_INDEX = 12;
	protected static final int OPAQUE_FIELD_LENGTH = 4;
	protected static final int CAS_INDEX = 16;
	protected static final int CAS_FIELD_LENGTH = 8;
	protected static final int HEADER_LENGTH = 24;
	
	protected byte[] mbytes;

	public BaseMessage() {
		mbytes = new byte[HEADER_LENGTH];
	}
	
	public final void setMagic(Magic m) {
		mbytes[MAGIC_INDEX] = (byte) m.magic;
	}
	
	public final void setOpcode(Opcode o) {
		mbytes[OPCODE_INDEX] = (byte) o.opcode;
	}
	
	public final byte getOpcode() {
		return mbytes[OPCODE_INDEX];
	}
	
	protected final void setKeylength(long l) {
		longToField(mbytes, KEY_LENGTH_INDEX, KEY_LENGTH_FIELD_LENGTH, l);
	}
	
	public final int getKeylength() {
		return (int) fieldToLong(mbytes, KEY_LENGTH_INDEX, KEY_LENGTH_FIELD_LENGTH);
	}
	
	public final void setDatatype(byte b) {
		mbytes[DATA_TYPE_INDEX] = b;
	}
	
	public final byte getDatatype() {
		return mbytes[DATA_TYPE_INDEX];
	}
	
	public final void setExtralength(int i) {
		mbytes[EXTRA_LENGTH_INDEX] = (byte) i;
	}
	
	public final int getExtralength() {
		return mbytes[EXTRA_LENGTH_INDEX];
	}
	
	public final void setVbucket(byte b) {
		
	}
	
	public final int getVbucket() {
		return (int) fieldToLong(mbytes, VBUCKET_INDEX, VBUCKET_FIELD_LENGTH);
	}
	
	public final void setTotalbody(long l) {
		longToField(mbytes, TOTAL_BODY_INDEX, TOTAL_BODY_FIELD_LENGTH, l);
	}
	
	public final int getTotalbody() {
		return (int) fieldToLong(mbytes, TOTAL_BODY_INDEX, TOTAL_BODY_FIELD_LENGTH);
	}
	
	public final void setOpaque(byte b) {
		
	}
	
	public final int getOpaque() {
		return (int) fieldToLong(mbytes, OPAQUE_INDEX, OPAQUE_FIELD_LENGTH);
	}
	
	public final void setCas(byte b) {
		
	}
	
	public final long getCas() {
		return fieldToLong(mbytes, CAS_INDEX, CAS_FIELD_LENGTH);
	}
	
	public final int getMessageLength() {
		return HEADER_LENGTH + getTotalbody();
	}
	
	public final ByteBuffer getBytes() {
		return ByteBuffer.wrap(mbytes);
	}
	
	public void printMessage() {
		int colNum = 0;
		System.out.printf("   %5s%5s%5s%5s\n", "0", "1", "2", "3");
		System.out.print("   ----------------------");
		for (int i = 0; i < mbytes.length; i++) {
			if ((i % 4) == 0) {
				System.out.printf("\n%3d|", colNum);
				colNum += 4;
			}
			int field = mbytes[i];
			if (field < 0)
				field = field + 256;
			System.out.printf("%5x", field);
		}
		System.out.print("\n\n");
	}
	
	public void printMessageDetails() {
		System.out.println("----------Message----------");
		System.out.printf("Magic: 0x%x\n", mbytes[MAGIC_INDEX]);
		System.out.printf("Opcode: 0x%x\n", mbytes[OPCODE_INDEX]);
		System.out.printf("Key Length: %d\n", getKeylength());
		System.out.printf("Extra Length: %d\n", getExtralength());
		System.out.printf("Data Type: 0x%x\n", getDatatype());
		System.out.printf("VBucket: %d\n", getVbucket());
		System.out.printf("Total Body: %d\n", getTotalbody());
		System.out.printf("Opaque: %d\n", getOpaque());
		System.out.printf("CAS: %d\n", getCas());
	}
	
	protected long fieldToLong(byte[] buffer, int offset, int length) {
		long total = 0;
		long val = 0;
		for (int i = 0; i < length; i++) {
			val = buffer[offset + i];
			if (val < 0)
				val = val + 256;
			total += (long)Math.pow(256.0, (double) (length - 1 - i)) * val;
		}
		return total;
	}
	
	protected void longToField(byte[] buffer, int offset, int length, long l) {
		long divisor;
		for (int i = 0; i < length; i++) {
			divisor = (long)Math.pow(256.0, (double) (length - 1 - i));
			buffer[offset + i] = (byte) (l / divisor);
			l = l % divisor;
		}
	}
}
