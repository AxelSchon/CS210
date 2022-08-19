package sql;

/**
 * Defines the protocols for a query error.
 * <p>
 * Do not modify existing protocols,
 * but you may add new protocols.
 */
@SuppressWarnings("serial")
public class QueryError extends Exception {
	public QueryError(String reason) {
		super(reason);
	}

	public QueryError(String reason, Throwable cause) {
		super(reason, cause);
	}
}
