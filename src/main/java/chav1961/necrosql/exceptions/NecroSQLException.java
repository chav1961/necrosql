package chav1961.necrosql.exceptions;

import java.sql.SQLException;

public class NecroSQLException extends SQLException {
	private static final long serialVersionUID = -5644137536640886611L;

	public NecroSQLException() {
	}

	public NecroSQLException(String message) {
		super(message);
	}

	public NecroSQLException(Throwable cause) {
		super(cause);
	}

	public NecroSQLException(String message, Throwable cause) {
		super(message, cause);
	}
}
