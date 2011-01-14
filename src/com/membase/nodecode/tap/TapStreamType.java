/*
 * Copyright (c) 2010 Membase. All Rights Reserved.
 */

package com.membase.nodecode.tap;

/**
 *
 */
public enum TapStreamType {
	BACK_FILL(0x01), DUMP(0x02);

	public int optionCode;

	TapStreamType(int optionCode) {
		this.optionCode = optionCode;
	}
}
