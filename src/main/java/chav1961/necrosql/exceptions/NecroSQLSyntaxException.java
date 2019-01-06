package chav1961.necrosql.exceptions;

public class NecroSQLSyntaxException extends NecroSQLException {
	private static final long serialVersionUID = -2391391358328408386L;

	public NecroSQLSyntaxException() {
	}

	public NecroSQLSyntaxException(String message) {
		super(message);
	}

	public NecroSQLSyntaxException(Throwable cause) {
		super(cause);
	}

	public NecroSQLSyntaxException(String message, Throwable cause) {
		super(message, cause);
	}
}
