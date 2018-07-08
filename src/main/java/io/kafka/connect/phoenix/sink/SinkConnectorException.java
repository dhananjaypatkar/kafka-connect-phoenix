package io.kafka.connect.phoenix.sink;

/**
 * 
 * @author Dhananjay
 *
 */
public class SinkConnectorException extends RuntimeException {

    /**
	 *
	 */
	private static final long serialVersionUID = -7544850650938270177L;

	public SinkConnectorException() {
        super();
    }

    public SinkConnectorException(String message) {
        super(message);
    }

    public SinkConnectorException(String message, Throwable cause) {
        super(message, cause);
    }

    public SinkConnectorException(Throwable cause) {
        super(cause);
    }
}
