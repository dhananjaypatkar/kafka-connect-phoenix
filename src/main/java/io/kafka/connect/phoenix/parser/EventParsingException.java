package io.kafka.connect.phoenix.parser;

/**
 * 
 * @author Dhananjay
 *
 */
public class EventParsingException extends RuntimeException {

    /**
	 *
	 */
	private static final long serialVersionUID = -5861884289109519422L;

	public EventParsingException() {
        super();
    }

    public EventParsingException(String message) {
        super(message);
    }

    public EventParsingException(String message, Throwable cause) {
        super(message, cause);
    }

    public EventParsingException(Throwable cause) {
        super(cause);
    }

}
