package sql;

import apps.Database;

/**
 * Defines the protocols for a driver.
 * <p>
 * Do not modify existing protocols,
 * but you may add new protocols.
 */
public interface Driver {
	/**
	 * If this driver recognizes the given query,
	 * parses it and returns <code>true</code>.
	 * <p>
	 * Otherwise, returns <code>false</code>.
	 *
	 * @param query a query to parse.
	 * @return whether the query is recognized.
	 *
	 * @throws QueryError
	 * if the query is recognized but can't be parsed.
	 **/
	boolean parse(String query) throws QueryError;

	/**
	 * Executes the most recently parsed query on
	 * the given database and returns the result.
	 * <p>
	 * The behavior of this operation is undefined
	 * except when it is called immediately after
	 * {@link #parse(String)} returns <code>true</code>.
	 *
	 * @param db the database to use.
	 * @return the result.
	 *
	 * @throws QueryError
	 * if the parsed query can't be executed.
	 **/
	Object execute(Database db) throws QueryError;
}