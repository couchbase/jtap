package com.membase.jtap.exception;

public class FieldDoesNotExistException extends RuntimeException{
	private static final long serialVersionUID = -9031043272617359144L;

	public FieldDoesNotExistException(String message) {
        super(message);
    }

    public FieldDoesNotExistException(String message, Throwable cause) {
        super(message, cause);
    }
}
