package chav1961.necrosql.exceptions;

public class NecroSQLCalculationException extends NecroSQLException {
	private static final long serialVersionUID = 7456301897524287861L;

	public NecroSQLCalculationException() {
	}

	public NecroSQLCalculationException(String message) {
		super(message);
	}

	public NecroSQLCalculationException(Throwable cause) {
		super(cause);
	}

	public NecroSQLCalculationException(String message, Throwable cause) {
		super(message, cause);
	}
}
