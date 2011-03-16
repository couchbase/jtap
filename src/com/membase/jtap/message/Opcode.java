package com.membase.jtap.message;

/**
 * The Opcode enum contains a list all of the different opcodes that can be passed in a tap message in the flag field.
 */
public enum Opcode {
	/**
	 * Defines a tap no-op message
	 */
	NOOP((byte) 0x0a),
	
	/**
	 * Defines a SASL list mechanism message
	 */
	SASLLIST((byte) 0x20),
	
	/**
	 * Defines a SASL authorization message
	 */
	SASLAUTH((byte) 0x21),
	
	/**
	 * Defines a request message to open a tap connection
	 */
	REQUEST((byte) 0x40),
	
	/**
	 * Defines a key-value mutation message to specify a key-value has changed
	 */
	MUTATION((byte) 0x41),
	
	/**
	 * Defines a delete message to specify a key has been deleted
	 */
	DELETE((byte) 0x42),
	
	/**
	 * Defines a tap flush message
	 */
	FLUSH((byte) 0x43),
	
	/**
	 * Defines a opaque message to send control data to the consumer
	 */
	OPAQUE((byte)0x44),
	
	/**
	 * Defines a vBucket set message to set the state of a vBucket in the consumer
	 */
	VBUCKETSET((byte) 0x45);

	/**
	 * The opcode value
	 */
	public byte opcode;

	/**
	 * Defines the magic value
	 * @param magic - The new magic value
	 */
	Opcode(byte opcode) {
		this.opcode = opcode;
	}
}
