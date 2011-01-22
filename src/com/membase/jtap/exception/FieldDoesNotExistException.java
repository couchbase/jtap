package com.membase.jtap.exception;

/**
 * The FieldDoesNotExistException defines an error caused when a trying to read a field from a tap
 * message that does not exist or was not instantiated.
 */
public class FieldDoesNotExistException extends RuntimeException{
	private static final long serialVersionUID = -9031043272617359144L;

	/**
	 * Constructs an FieldDoesNotExistException with null as its error detail message.
	 */
	public FieldDoesNotExistException() {
        super();
    }
	
	/**
	 * Constructs an FieldDoesNotExistException with the specified detail message. 
	 * The error message string s can later be retrieved by the Throwable.getMessage()
	 * method of class java.lang.Throwable.
	 * @param message - the detail message
	 */
	public FieldDoesNotExistException(String message) {
        super(message);
    }
}
