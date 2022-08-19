package tables;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import sql.FieldType;

/**
 * Implements a search-based table
 * using a list data structure.
 */
public class SearchTable extends Table {
	/*
	 * TODO: Implement stub for Practice 1 (optional)
	 * by replacing linear search with binary search.
	 */

	private List<List<Object>> list;
	private int fingerprint;

	/**
	 * Creates a table and initializes
	 * the data structure.
	 *
	 * @param tableName the table name
	 * @param columnNames the column names
	 * @param columnTypes the column types
	 * @param primaryIndex the primary index
	 */
	public SearchTable(String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex) {
		setTableName(tableName);
		setColumnNames(columnNames);
		setColumnTypes(columnTypes);
		setPrimaryIndex(primaryIndex);

		list = new LinkedList<>();
		clear();
	}

	@Override
	public void clear() {
		list.clear();
		fingerprint = 0;
	}

	@Override
	public boolean put(List<Object> row) {
		row = sanitizeRow(row);

		Object key = row.get(getPrimaryIndex());
		for (int i = 0; i < list.size(); i++) {
			List<Object> old = list.get(i);
			if (old.get(getPrimaryIndex()).equals(key)) {
				fingerprint += row.hashCode() - old.hashCode();
				list.set(i, row);
				return true;
			}
		}
		fingerprint += row.hashCode();
		list.add(row);
		return false;
	}

	@Override
	public boolean remove(Object key) {
		for (int i = 0; i < list.size(); i++) {
			List<Object> row = list.get(i);
			if (row.get(getPrimaryIndex()).equals(key)) {
				fingerprint -= row.hashCode();
				list.remove(i);
				return true;
			}
		}
		return false;
	}

	@Override
	public List<Object> get(Object key) {
		for (List<Object> row: list) {
			if (row.get(getPrimaryIndex()).equals(key))
				return row;
		}

		return null;
	}

	@Override
	public int size() {
		return list.size();
	}

	@Override
	public int capacity() {
		return size();
	}

	@Override
	public int hashCode() {
		return fingerprint;
	}

	@Override
	public Iterator<List<Object>> iterator() {
		return new Iterator<>() {
			int index = 0;

			@Override
			public boolean hasNext() {
				return index < list.size();
			}

			@Override
			public List<Object> next() {
				return list.get(index++);
			}
		};
	}
}