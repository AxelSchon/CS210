package tables;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import sql.FieldType;

/**
 * Implements a hash-based table using an array data structure.
 */
public class HashArrayTable extends Table {
	private Object[] array;
	private int size;
	private int fingerprint;
	// private int contamination; // OA

	// constants
	private static final int MIN_CAPACITY = 19; // prime number < 20
	private static final int LOAD_FACTOR_BOUND = 5; // SC
	// private static final double LOAD_FACTOR_BOUND = 0.75; // OA
	// private static final Object TOMBSTONE = new Object(); // OA

	/**
	 * Creates a table and initializes the data structure.
	 *
	 * @param tableName    the table name
	 * @param columnNames  the column names
	 * @param columnTypes  the column types
	 * @param primaryIndex the primary index
	 */
	public HashArrayTable(String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex) {
		setTableName(tableName);
		setColumnNames(columnNames);
		setColumnTypes(columnTypes);
		setPrimaryIndex(primaryIndex);

		clear();

	}

	@Override
	public void clear() {
		array = new Object[MIN_CAPACITY];
		size = 0;
		fingerprint = 0;
		// contamination = 0; // OA
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean put(List<Object> row) {
		row = sanitizeRow(row);

		Object key = row.get(getPrimaryIndex());
		final int hash = hashFunction1(key);
		final int c = hashFunction2(key); // DH: step size

		int i = wrap(hash);
		//chain = array[i]
		for (int j = 0; j < LOAD_FACTOR_BOUND; j++) {
			//chain.get(j)

			List<Object> old = (List<Object>) array[i];

			// for separate chaining look at chain[i] instead of array[i]
			if (array[i] == null) { // miss
				array[i] = row;
				size++;
				if (old != null) {
					fingerprint += row.hashCode() - old.hashCode();
				}

				//if necessary, rehash

				return false;
			}

			if (old.get(getPrimaryIndex()).equals(key)) { // hit
				array[i] = row;
				size++;
				if (old != null) {
					fingerprint += row.hashCode() - old.hashCode();
				}
				return true;
			}
		}
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
		return size;
	}

	@Override
	public int capacity() {
		return array.length;
	}

	/**
	 * OA @ Override public double loadFactor() { return (double) (size +
	 * contamination / (double) array.length; }
	 */

	@Override
	public double loadFactor() {
		return (double) size / (double) array.length;
	}

	private static int hashFunction1(Object key) {
		String input = "%s-%s-%s".formatted("saltOfMyChoice", key.hashCode(), key.toString());

		// FNV 32-bit hash
		byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
		int FNV_offset_basis = 0x811c9dc5;
		int FNV_prime = 0x01000193;
		int hash = FNV_offset_basis; // initialize hash to FNV_offset_basis

		// add to the hash according to the algorithm
		for (int i = 0; i < bytes.length; i++) {
			hash = hash * FNV_prime;
			hash = hash ^ bytes[i];
		}
		return hash;
	}

	// hash function 2 for double hashing
	private static int hashFunction2(Object key) {
		String input = "%s-%s-%s".formatted("otherSaltOfMyChoice", key.hashCode(), key.toString());

		// polynomial Rolling Hash
		char[] chars = input.toCharArray();
		int hash = 0; // initialize hash

		// add to the hash according to the algorithm
		for (int i = 0; i < input.length(); i++) {
			hash = 31 * hash + chars[i]; // need to modulo by chars.length?
		}

		return hash;

	}

	// corrected mod function
	private int wrap(int index) {
		// index % array.length
		return Math.floorMod(index, array.length);
	}

	@Override
	public int hashCode() {
		return fingerprint;
	}

	@Override
	public Iterator<List<Object>> iterator() {
		return null;
	}
}
