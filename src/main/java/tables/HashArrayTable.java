package tables;

import java.util.Iterator;
import java.util.List;

import sql.FieldType;

/**
 * Implements a hash-based table
 * using an array data structure.
 */
public class HashArrayTable extends Table {
	/*
	 * TODO: Implement stub for Module 1.
	 */

	/**
	 * Creates a table and initializes
	 * the data structure.
	 *
	 * @param tableName the table name
	 * @param columnNames the column names
	 * @param columnTypes the column types
	 * @param primaryIndex the primary index
	 */
	public HashArrayTable(String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex) {
		throw new UnsupportedOperationException("Hash array table creation is unimplemented");
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
