package tables;

import java.util.Iterator;
import java.util.List;

import sql.FieldType;

/**
 * Implements a hash-based table
 * using a directory tree structure.
 */
public class HashFileTable extends Table {
	/*
	 * TODO: Implement stub for Module 2.
	 */

	/**
	 * Creates a table and initializes
	 * the file structure.
	 *
	 * @param tableName a table name
	 * @param columnNames the column names
	 * @param columnTypes the column types
	 * @param primaryIndex the primary index
	 */
	public HashFileTable(String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex) {
		throw new UnsupportedOperationException("Hash file table creation is unimplemented");
	}

	/**
	 * Reopens a table from an
	 * existing file structure.
	 *
	 * @param tableName a table name
	 */
	public HashFileTable(String tableName) {
		throw new UnsupportedOperationException("Hash file table reopening is unimplemented");
	}

	@Override
	public void clear() {

	}

	@Override
	public boolean put(List<Object> row) {
		return false;
	}

	@Override
	public boolean remove(Object key) {
		return false;
	}

	@Override
	public List<Object> get(Object key) {
		return null;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public int capacity() {
		return 0;
	}

	@Override
	public int hashCode() {
		return 0;
	}

	@Override
	public Iterator<List<Object>> iterator() {
		return null;
	}
}
