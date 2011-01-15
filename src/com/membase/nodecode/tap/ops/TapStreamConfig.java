/*
 * Copyright (c) 2010 Membase. All Rights Reserved.
 */

package com.membase.nodecode.tap.ops;

import com.membase.nodecode.tap.TapStreamConfiguration;
import com.membase.nodecode.tap.message.TapStreamMessage;

/**
 *
 */
public interface TapStreamConfig {
	TapStreamConfiguration getConfiguration();

	void receive(TapStreamMessage streamMessage);
	
	TapStreamMessage getMessage();
}
