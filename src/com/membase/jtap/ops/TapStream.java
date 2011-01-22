/*
 * Copyright (c) 2010 Membase. All Rights Reserved.
 */

package com.membase.jtap.ops;

import com.membase.jtap.message.RequestMessage;
import com.membase.jtap.message.ResponseMessage;

/**
 * TapStream specifies the major pieces of functionality that a tap stream must contain.
 */
public interface TapStream {
	
	/**
	 * Returns an object that contains a representation of the tap stream message that initiates the
	 * tap stream.
	 */
	void receive(ResponseMessage streamMessage);
	
	/**
	 * Specifies how a received tap stream message will interact with the streams exporter.
	 * @param streamMessage The message received from the Membase.
	 */
	RequestMessage getMessage();
	
	/**
	 * Returns the number of messages that this tap stream has handled.
	 */
	long getCount();
}
