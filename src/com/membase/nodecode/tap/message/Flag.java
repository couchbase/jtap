package com.membase.nodecode.tap.message;

public enum Flag {
	BACKFILL((byte) 0x01),
	DUMP((byte) 0x02),
	LIST_VBUCKETS((byte) 0x04),
	TAKEOVER_VBUCKETS((byte) 0x08),
	SUPPORT_ACK((byte) 0x10),
	KEYS_ONLY((byte) 0x20);
	
	public byte flag;

	Flag(byte flag) {
		this.flag = flag;
	}
}
