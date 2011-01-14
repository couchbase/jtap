/*
 * Copyright (c) 2010 Membase. All Rights Reserved.
 */

package com.membase.nodecode.tap.ops;

import com.membase.nodecode.tap.TapStreamConfiguration;
import com.membase.nodecode.tap.message.TapStreamMessage;

/**
 *
 */
public interface TapStream {
	TapStreamConfiguration getConfiguration();

	void prepare();

	void receive(TapStreamMessage streamMessage);

	void cleanup();
	
	TapStreamMessage getMessage();
}
