package apps;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import drivers.CreateTable;
import drivers.DropTable;
import drivers.Echo;
import drivers.InsertInto;
import drivers.Macros;
import drivers.Range;
import drivers.ShowTable;
import drivers.ShowTables;
import drivers.TimesTable;
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

	private final List<Table> tables; // list of tables we are storing
	private final boolean persistent; // whether or not data is supposed to be persistent

	/**
	 * Initializes the tables.
	 *
	 * @param persistent whether the database is persistent.
	 */
	public Database(boolean persistent) {
		this.persistent = persistent; // sets persistence to passed boolean

		tables = new LinkedList<>(); // tables is just a new linked list
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
	public boolean create(Table table) { // can never have two tables with same name
		if (exists(table.getTableName()))
			return false;

		tables.add(table);
		return true;
	}

	/**
	 * Drops the table with the given name,
	 * unless no table with that name exists.
	 *
	 * @param tableName a table name.
	 * @return the dropped table, if any.
	 */
	public Table drop(String tableName) { // can only drop names which are in use
		for (var i = 0; i < tables.size(); i++)
			if (tables.get(i).getTableName().equals(tableName))
				return tables.remove(i);
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
		for (Table table : tables)
			if (table.getTableName().equals(tableName))
				return table;
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
		return find(tableName) != null;
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
	 *                    if some driver can't parse or execute the query,
	 *                    or if no driver recognizes the query.
	 */
	@SuppressWarnings("deprecation")
	public Object interpret(String query) throws QueryError { // works by plugging in a bunch of components which know how to handle each on of the different query types and asks those to do the work

		// TODO make a list of new driver objects, loop through it
		Driver create = new CreateTable();
		if (create.parse(query))
			return create.execute(this);

		Driver drop = new DropTable();
		if (drop.parse(query))
			return drop.execute(this);

		Driver echo = new Echo();
		if (echo.parse(query))
			return echo.execute(null);

		Driver insert = new InsertInto();
		if (insert.parse(query))
			return insert.execute(this);

		Driver macro = new Macros();
		if (macro.parse(query))
			return macro.execute(this);

		Driver range = new Range();
		if (range.parse(query))
			return range.execute(null);

		Driver show = new ShowTable();
		if (show.parse(query))
			return show.execute(this);

		Driver shows = new ShowTables();
		if (shows.parse(query))
			return shows.execute(this);

		Driver timesTable = new TimesTable();
		if (timesTable.parse(query))
			return timesTable.execute(this);

		throw new QueryError("Unrecognized query");
	}

	/**
	 * Performs any required tasks when
	 * the database is closed (optional).
	 */
	@Override
	public void close() throws IOException {
		// don't need to implement
	}
}
