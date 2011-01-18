package com.membase.jtap.message;

public enum Magic {
	PROTOCOL_BINARY_REQ((byte) 0x80);

	public byte optionCode;

	Magic(byte optionCode) {
		this.optionCode = optionCode;
	}
}
