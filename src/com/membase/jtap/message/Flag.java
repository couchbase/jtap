package com.membase.jtap.message;

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
	
	boolean hasFlag(int f) {
		int bit = 0;
		for (int i = 1; i <= this.flag; i *= 2) {
			bit = f % 2;
			f = f / 2;
		}
		if (bit == 0)
			return false;
		return true;
	}
}
