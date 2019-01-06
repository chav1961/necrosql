package chav1961.necrosql.exceptions;

public class NecroSQLLowLayerException extends NecroSQLException {
	private static final long serialVersionUID = -6823091185077917819L;

	public NecroSQLLowLayerException() {
	}

	public NecroSQLLowLayerException(String message) {
		super(message);
	}

	public NecroSQLLowLayerException(Throwable cause) {
		super(cause);
	}

	public NecroSQLLowLayerException(String message, Throwable cause) {
		super(message, cause);
	}
}
