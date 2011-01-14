/*
 * Copyright (c) 2010 Membase. All Rights Reserved.
 */

package com.membase.nodecode.tap.message;

import java.nio.ByteBuffer;

/**
 *
 */
public class TapStreamMessage {
	private static final int MAGIC_INDEX = 0;
	private static final int MAGIC_FIELD_LENGTH = 1;
	private static final int OPCODE_INDEX = MAGIC_INDEX + MAGIC_FIELD_LENGTH;
	private static final int OPCODE_FIELD_LENGTH = 1;
	private static final int KEY_LENGTH_INDEX = OPCODE_INDEX + OPCODE_FIELD_LENGTH;
	private static final int KEY_LENGTH_FIELD_LENGTH = 2;
	private static final int EXTRA_LENGTH_INDEX = KEY_LENGTH_INDEX + KEY_LENGTH_FIELD_LENGTH;
	private static final int EXTRA_LENGTH_FIELD_LENGTH = 1;
	private static final int DATA_TYPE_INDEX = EXTRA_LENGTH_INDEX + EXTRA_LENGTH_FIELD_LENGTH;
	private static final int DATA_TYPE_FIELD_LENGTH = 1;
	private static final int VBUCKET_INDEX = DATA_TYPE_INDEX + DATA_TYPE_FIELD_LENGTH;
	private static final int VBUCKET_FIELD_LENGTH = 2;
	private static final int TOTAL_BODY_INDEX = VBUCKET_INDEX + VBUCKET_FIELD_LENGTH;
	private static final int TOTAL_BODY_FIELD_LENGTH = 4;
	private static final int OPAQUE_INDEX = TOTAL_BODY_INDEX + TOTAL_BODY_FIELD_LENGTH;
	private static final int OPAQUE_FIELD_LENGTH = 4;
	private static final int CAS_INDEX = OPAQUE_INDEX + OPAQUE_FIELD_LENGTH;
	private static final int CAS_FIELD_LENGTH = 8;
	private static final int FLAGS_INDEX = CAS_INDEX + CAS_FIELD_LENGTH;
	private static final int FLAGS_FIELD_LENGTH = 4;
	
	private static final int MIN_HEADER_FIELD_LENGTH = 28;
	
	private byte magic;
	private byte opcode;
	private byte[] keylength;
	private byte extralength;
	private byte datatype;
	private byte[] vbucket;
	private byte[] totalbody;
	private byte[] opaque;
	private byte[] cas;
	private byte[] flags;
	private byte[] name;

	public TapStreamMessage() {
		magic = (byte) 0;
		opcode = (byte) 0;
		keylength = new byte[KEY_LENGTH_FIELD_LENGTH];
		extralength = (byte) 0;
		datatype = (byte) 0;
		vbucket = new byte[VBUCKET_FIELD_LENGTH];
		totalbody = new byte[TOTAL_BODY_FIELD_LENGTH];
		opaque = new byte[OPAQUE_FIELD_LENGTH];
		cas = new byte[CAS_FIELD_LENGTH];
		flags = new byte[FLAGS_FIELD_LENGTH];
		name = null;
	}
	
	public void setMagic(Magic m) {
		magic = (byte) m.optionCode;
	}
	
	public void setOpcode(Opcode o) {
		opcode = (byte) o.opcode;
	}
	
	public long getKeylength() {
		return fieldToInt(keylength);
	}
	
	public void setDatatype(byte b) {
		datatype = b;
	}
	
	public byte getDatatype() {
		return datatype;
	}
	
	public void setExtralength(int i) {
		extralength = (byte) i;
	}
	
	public long getExtralength() {
		return (long) extralength;
	}
	
	public void setVbucket(byte b) {
		
	}
	
	public long getVbucket() {
		return fieldToInt(vbucket);
	}
	
	public void setTotalbody(int i) {
		totalbody[3] = (byte) i;
	}
	
	public long getTotalbody() {
		return fieldToInt(totalbody);
	}
	
	public void setOpaque(byte b) {
		
	}
	
	public long getOpaque() {
		return fieldToInt(opaque);
	}
	
	public void setCas(byte b) {
		
	}
	
	public long getCas() {
		return fieldToInt(cas);
	}
	
	public void setFlags(Flag[] f) {
		assert f.length <= 5;
		for (int i = 0; i < f.length; i++)
			flags[FLAGS_FIELD_LENGTH - i - 1] = f[i].flag;
	}
	
	public int getMessageLength() {
		return MIN_HEADER_FIELD_LENGTH + name.length;
	}
	
	public void setName(String s) {
		// TODO: This needs to throw an exception
		int len = s.length();
		if (len >= (int)Math.pow((double) 2, (double) (KEY_LENGTH_FIELD_LENGTH * 8)))
			System.out.println("Name too big");
		name = s.getBytes();
		keylength[0] = (byte) (len / 256);
		keylength[1] = (byte) (len % 256);
	}
	
	public ByteBuffer encode() {
		int len = getMessageLength();
		byte[] buffer = new byte[len];
		
		buffer[MAGIC_INDEX] = magic;
		buffer[OPCODE_INDEX] = opcode;
		
		for (int i = 0; i < KEY_LENGTH_FIELD_LENGTH; i++)
			buffer[KEY_LENGTH_INDEX + i] = keylength[i];
		
		buffer[EXTRA_LENGTH_INDEX] = extralength;
		buffer[DATA_TYPE_INDEX] = datatype;
		
		for(int i = 0; i < VBUCKET_FIELD_LENGTH; i++)
			buffer[VBUCKET_INDEX + i] = vbucket[i];
		
		for(int i = 0; i < TOTAL_BODY_FIELD_LENGTH; i++)
			buffer[TOTAL_BODY_INDEX + i] = totalbody[i];
		
		for(int i = 0; i < OPAQUE_FIELD_LENGTH; i++)
			buffer[OPAQUE_INDEX + i] = opaque[i];
		
		for(int i = 0; i < CAS_FIELD_LENGTH; i++)
			buffer[CAS_INDEX + i] = cas[i];
		
		for(int i = 0; i < FLAGS_FIELD_LENGTH; i++)
			buffer[FLAGS_INDEX + i] = flags[i];
		
		for(int i = 0; i < name.length; i++)
			buffer[FLAGS_INDEX + FLAGS_FIELD_LENGTH + i] = name[i];
		
		return ByteBuffer.wrap(buffer);
	}
	
	public void decode(byte[] buffer) {
		magic = buffer[MAGIC_INDEX];
		opcode = buffer[OPCODE_INDEX];
		
		for(int i = 0; i < KEY_LENGTH_FIELD_LENGTH; i++)
			keylength[i] = buffer[KEY_LENGTH_INDEX + i];
		
		extralength = buffer[EXTRA_LENGTH_INDEX];
		datatype = buffer[DATA_TYPE_INDEX];
		
		for(int i = 0; i < KEY_LENGTH_FIELD_LENGTH; i++)
			vbucket[i] = buffer[VBUCKET_INDEX + i];
		
		for(int i = 0; i < TOTAL_BODY_FIELD_LENGTH; i++)
			totalbody[i] = buffer[TOTAL_BODY_INDEX + i];
		
		for(int i = 0; i < OPAQUE_FIELD_LENGTH; i++)
			opaque[i] = buffer[OPAQUE_INDEX + i];
		
		for(int i = 0; i < CAS_FIELD_LENGTH; i++)
			cas[i] = buffer[CAS_INDEX + i];
		
		for(int i = 0; i < FLAGS_FIELD_LENGTH; i++)
			flags[i] = buffer[FLAGS_INDEX + i];
	}
	
	public void printMessage() {
		printMessage(encode(), getMessageLength());
	}
	
	public static void printMessage(ByteBuffer buffer, int messageLength) {
		int colNum = 0;
		
		System.out.printf("   %5s%5s%5s%5s\n", "0", "1", "2", "3");
		System.out.print("   ----------------------");
		for (int i = 0; i < messageLength; i++) {
			if ((i % 4) == 0) {
				System.out.printf("\n%3d|", colNum);
				colNum += 4;
			}
			int field = buffer.get(i);
			if (field < 0)
				field = field + 256;
			System.out.printf("%5x", field);
		}
		buffer.position(0);
		System.out.print("\n\n");
	}
	
	public void printMessageDetails() {
		System.out.println("----------Message----------");
		System.out.printf("Magic: 0x%x\n", magic);
		System.out.printf("Opcode: 0x%x\n", opcode);
		System.out.printf("Key Length: %d\n", getKeylength());
		System.out.printf("Extra Length: %d\n", getExtralength());
		System.out.printf("Data Type: 0x%x\n", datatype);
		System.out.printf("VBucket: %d\n", getVbucket());
		System.out.printf("Total Body: %d\n", getTotalbody());
		System.out.printf("Opaque: %d\n", getOpaque());
		System.out.printf("CAS: %d\n", getCas());
		System.out.println("---------------------------");
	}
	
	private long fieldToInt(byte[] ba) {
		long total = 0;
		for (int i = 0; i < ba.length; i++) {
			total += (long)Math.pow((double) 256, (double) (ba.length - 1 - i)) * (long)ba[i];
		}
		return total;
	}
}
