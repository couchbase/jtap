/*
 * Copyright (c) 2010 Membase. All Rights Reserved.
 */

package com.membase.nodecode.tap.internal;

import com.membase.nodecode.tap.TapStreamType;

/**
 *
 */
public class InitStreamRequest {
	public TapStreamType option;
	public String nodeName;
	public long startDate;

	public InitStreamRequest(TapStreamType option, String nodeName) {
		this.option = option;
		this.nodeName = nodeName;
	}

	public InitStreamRequest(TapStreamType option, String nodeName,
			long startDate) {
		this.option = option;
		this.nodeName = nodeName;
		this.startDate = startDate;
	}
}
