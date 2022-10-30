package it.eng.anas.db;


@SuppressWarnings("serial")
public class SimpleDbOpException extends RuntimeException {
	public SimpleDbOpException(String msg) {
		super(msg);
	}
	public SimpleDbOpException(String msg, Throwable cause) {
		super(msg, cause);
	}
	public SimpleDbOpException(Throwable cause) {
		super(cause);
	}

}
