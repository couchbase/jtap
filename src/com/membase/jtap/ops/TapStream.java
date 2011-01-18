/*
 * Copyright (c) 2010 Membase. All Rights Reserved.
 */

package com.membase.jtap.ops;

import com.membase.jtap.TapStreamConfiguration;
import com.membase.jtap.message.RequestMessage;
import com.membase.jtap.message.ResponseMessage;

/**
 *
 */
public interface TapStream {
	TapStreamConfiguration getConfiguration();

	void receive(ResponseMessage streamMessage);
	
	RequestMessage getMessage();
}
