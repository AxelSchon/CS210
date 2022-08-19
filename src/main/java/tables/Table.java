package tables;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import sql.FieldType;

/**
 * Defines the protocols for a table
 * with a schema and a state.
 * <p>
 * Do not modify existing protocols,
 * but you may add new protocols.
 */
public abstract class Table implements Iterable<List<Object>> {
	private String tableName;
	private List<String> columnNames;
	private List<FieldType> columnTypes;
	private int primaryIndex;

	/**
	 * Sets the table name in the schema.
	 *
	 * @param tableName the table name.
	 */
	public void setTableName(String tableName) {
		if (!tableName.matches("[a-zA-Z0-9_]{1,15}"))
			throw new IllegalArgumentException("Table name <%s> must be valid".formatted(tableName));

		this.tableName = tableName;
	}

	/**
	 * Gets the table name from the schema.
	 *
	 * @return the table name.
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * Sets an unmodifiable list of
	 * the column names in the schema.
	 *
	 * @param columnNames the column names.
	 */
	public void setColumnNames(List<String> columnNames) {
		if (columnNames.size() < 1 || columnNames.size() > 15)
			throw new IllegalArgumentException("Number of column types <%d> must be from 1 to 15".formatted(columnNames.size()));

		for (int i = 0; i < columnNames.size(); i++) {
			String name = columnNames.get(i);
			if (!name.matches("[a-zA-Z0-9_]{1,15}"))
				throw new IllegalArgumentException("Column name <%s> at index <%d> must be valid".formatted(name, i));
			if (columnNames.indexOf(name) != i)
				throw new IllegalArgumentException("Duplicate column name <%s>".formatted(name));
		}

		this.columnNames = List.copyOf(columnNames);
	}

	/**
	 * Gets an unmodifiable list of
	 * the column names from the schema.
	 *
	 * @return the column names.
	 */
	public List<String> getColumnNames() {
		return columnNames;
	}

	/**
	 * Sets an unmodifiable list of
	 * the column types in the schema.
	 *
	 * @param columnTypes the column types.
	 */
	public void setColumnTypes(List<FieldType> columnTypes) {
		if (columnTypes.size() != columnNames.size())
			throw new IllegalArgumentException("Number of column types <%d> must match number of column names <%d>".formatted(columnTypes.size(), columnNames.size()));

		for (int i = 0; i < columnTypes.size(); i++) {
			FieldType type = columnTypes.get(i);
			switch (type) {
				case STRING, INTEGER, BOOLEAN -> {}
				default -> throw new IllegalArgumentException("Unknown type <%s>".formatted(type));
			}
		}

		this.columnTypes = List.copyOf(columnTypes);
	}

	/**
	 * Gets an unmodifiable list of
	 * the column types from the schema.
	 *
	 * @return the column types.
	 */
	public List<FieldType> getColumnTypes() {
		return columnTypes;
	}

	/**
	 * Sets the primary index in the schema.
	 *
	 * @param primaryIndex the primary index.
	 */
	public void setPrimaryIndex(int primaryIndex) {
		if (primaryIndex < 0 || primaryIndex >= columnNames.size())
			throw new IllegalArgumentException("Primary index <%d> must be from 0 to %d".formatted(primaryIndex, columnNames.size() - 1));

		this.primaryIndex = primaryIndex;
	}

	/**
	 * Gets the primary index from the schema.
	 *
	 * @return the primary index.
	 */
	public int getPrimaryIndex() {
		return primaryIndex;
	}

	/**
	 * Removes all rows from the state.
	 */
	public abstract void clear();

	/**
	 * On a hit, sanitizes and updates the corresponding row
	 * in the state, then returns <code>true</code>.
	 * <p>
	 * On a miss, sanitizes and creates the given
	 * row in the state, then returns <code>false</code>.
	 *
	 * @param row a row.
	 * @return whether the operation was a hit.
	 *
	 * @throws RuntimeException
	 * if the row is not valid.
	 */
	public abstract boolean put(List<Object> row);

	/**
	 * Asserts the given row is schematically valid,
	 * then returns a copy of it.
	 *
	 * @throws NullPointerException
	 * if the row or key is null.
	 *
	 * @throws IllegalArgumentException
	 * if the row has the wrong number or types of fields.
	 *
	 * @param row
	 */
	public List<Object> sanitizeRow(List<Object> row) {
		if (row == null)
			throw new NullPointerException("Row must not be null");

		if (row.size() != getColumnNames().size())
			throw new IllegalArgumentException("Row must have %d fields".formatted(getColumnNames().size()));

		if (row.get(getPrimaryIndex()) == null)
			throw new NullPointerException("Key field %d must not be null".formatted(getPrimaryIndex()));

		for (int i = 0; i < row.size(); i++) {
			Object field = row.get(i);
			if (field == null)
				continue;

			FieldType type = getColumnTypes().get(i);
			switch (type) {
				case STRING -> {
					if (!(field instanceof String))
						throw new IllegalArgumentException("Field <%d> must be a string".formatted(i));
				}
				case INTEGER -> {
					if (!(field instanceof Integer))
						throw new IllegalArgumentException("Field <%d> must be an integer".formatted(i));
				}
				case BOOLEAN -> {
					if (!(field instanceof Boolean))
						throw new IllegalArgumentException("Field <%d> must be a boolean".formatted(i));
				}
				default -> throw new IllegalArgumentException("Unexpected type <%s>".formatted(type));
			}
		}

		return new LinkedList<>(row);
	}

	/**
	 * Tries to {@link #put(List)} each row
	 * from the given iterable of rows.
	 *
	 * @param rows an iterable of rows.
	 */
	public void putAll(Iterable<List<Object>> rows) {
		for (List<Object> row: rows)
			put(row);
	}

	/**
	 * On a hit, removes the corresponding row
	 * from the state, then returns <code>true</code>.
	 * <p>
	 * On a miss, returns <code>false</code>.
	 *
	 * @param key a key.
	 * @return whether the operation was a hit.
	 */
	public abstract boolean remove(Object key);

	/**
	 * On a hit, returns the corresponding row
	 * from the state.
	 * <p>
	 * On a miss, returns <code>null</code>.
	 *
	 * @param key a key.
	 * @return whether the operation was a hit.
	 */
	public abstract List<Object> get(Object key);

	/**
	 * On a hit, returns <code>true</code>.
	 * <p>
	 * On a miss, returns <code>false</code>.
	 *
	 * @param key a key.
	 * @return whether the operation was a hit.
	 */
	public boolean contains(Object key) {
		return get(key) != null;
	}

	/**
	 * Returns the size of the table, which is
	 * the number of rows in the state.
	 *
	 * @return the size of the table.
	 */
	public abstract int size();

	/**
	 * Returns whether the {@link #size()} is zero.
	 *
	 * @return <code>true</code> if there are no rows
	 * 		or <code>false</code> if there are rows.
	 */
	public boolean isEmpty() {
		return size() == 0;
	}

	/**
	 * Returns the capacity of the table, which is
	 * the length of the data/file structure or
	 * the maximum size before resizing it.
	 *
	 * @return the capacity of the table.
	 */
	public abstract int capacity();

	/**
	 * Returns the load factor of the table,
	 * which is the {@link #size()}
	 * divided by the {@link #capacity()}.
	 *
	 * @return the load factor.
	 */
	public double loadFactor() {
		return (double) size() / (double) capacity();
	}

	/**
	 * Returns a string representation of this table,
	 * including its schema and state.
	 *
	 * @return a string representation of this table.
	 */
	@Override
	public String toString() {
		return "Table<Schema=[tableName=%s, columnNames=%s, columnTypes=%s, primaryIndex=%d], State=%s>".formatted(
			tableName, columnNames, columnTypes, primaryIndex, rows()
		);
	}

	/**
	 * Returns whether the given object is also a table
	 * and has the same fingerprint as this table.
	 * <p>
	 * A <code>true</code> result indicates
	 * with near certainty that the given object
	 * is a table with an equal state.
	 *
	 * @param an object.
	 * @return whether the given object equals this table.
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Table table)
			return this.hashCode() == table.hashCode();
		else return false;
	}

	/**
	 * Returns the fingerprint of this table,
	 * which is the sum of the hashcodes of each
	 * row in the state, disregarding the schema.
	 *
	 * @return this table's fingerprint.
	 */
	@Override
	public abstract int hashCode();

	/**
	 * Returns an iterator over each row in the state.
	 * <p>
	 * This method is an alias of {@link #rows()}.
	 *
	 * @return an iterator of rows.
	 */
	@Override
	public abstract Iterator<List<Object>> iterator();

	/**
	 * Returns an unmodifiable set of
	 * the rows in the state.
	 *
	 * @return the set of rows.
	 */
	public Set<List<Object>> rows() {
		Set<List<Object>> rows = new HashSet<>();
		for (List<Object> row: this)
			rows.add(row);
		return Set.copyOf(rows);
	}

	/**
	 * Returns an unmodifiable set of
	 * the keys of the rows in the state.
	 *
	 * @return the set of keys.
	 */
	public Set<Object> keys() {
		Set<Object> keys = new HashSet<>();
		for (List<Object> row: this)
			keys.add(row.get(getPrimaryIndex()));
		return Set.copyOf(keys);
	}
}