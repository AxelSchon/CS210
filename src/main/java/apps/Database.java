package apps;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import drivers.Echo;
import drivers.Range;
import drivers.ShowTable;
import sql.Driver;
import sql.QueryError;
import tables.Table;

/**
 * This class implements a
 * database management system.
 * <p>
 * Do not modify existing protocols,
 * but you may add new protocols.
 */
public class Database implements Closeable {
	/*
	 * TODO: Implement stub for Module 3.
	 */

	private final List<Table> tables;
	private final boolean persistent;

	/**
	 * Initializes the tables.
	 *
	 * @param persistent whether the database is persistent.
	 */
	public Database(boolean persistent) {
		this.persistent = persistent;

		tables = new LinkedList<>();
	}

	/**
	 * Returns whether the database is persistent.
	 *
	 * @return whether the database is persistent.
	 */
	public boolean isPersistent() {
		return persistent;
	}

	/**
	 * Returns an unmodifiable list
	 * of the tables in the database.
	 *
	 * @return the list of tables.
	 */
	public List<Table> tables() {
		return List.copyOf(tables);
	}

	/**
	 * Creates the given table,
	 * unless a table with its name exists.
	 *
	 * @param table a table.
	 * @return whether the table was created.
	 */
	public boolean create(Table table) {
		return false;
	}

	/**
	 * Drops the table with the given name,
	 * unless no table with that name exists.
	 *
	 * @param tableName a table name.
	 * @return the dropped table, if any.
	 */
	public Table drop(String tableName) {
		return null;
	}

	/**
	 * Returns the table with the given name,
	 * or <code>null</code> if there is none.
	 *
	 * @param tableName a table name.
	 * @return the named table, if any.
	 */
	public Table find(String tableName) {
		return null;
	}

	/**
	 * Returns whether a table
	 * with the given name exists.
	 *
	 * @param tableName a table name.
	 * @return whether the named table exists.
	 */
	public boolean exists(String tableName) {
		return false;
	}

	/**
	 * Interprets the given query on this database
	 * and returns the result.
	 *
	 * @param query a query to interpret.
	 * @return the result.
	 * @throws Exception
	 *
	 * @throws QueryError
	 * if some driver can't parse or execute the query,
	 * or if no driver recognizes the query.
	 */
	public Object interpret(String query) throws QueryError {
		Driver echo = new Echo();
		if (echo.parse(query))
			return echo.execute(null);

		Driver range = new Range();
		if (range.parse(query))
			return range.execute(null);

		Driver show = new ShowTable();
		if (show.parse(query))
			return show.execute(this);

		throw new QueryError("Unrecognized query");
	}

	/**
	 * Performs any required tasks when
	 * the database is closed (optional).
	 */
	@Override
	public void close() throws IOException {

	}
}
