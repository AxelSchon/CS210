package tables;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import sql.FieldType;

/**
 * Implements a hash-based table using an array data structure.
 */
public class HashArrayTable extends PrettyTable {
	private Object[] array;
	private int size;
	private int fingerprint;
	private int contamination;

	// constants
	private static final int MIN_CAPACITY = 19; // prime number < 20
	private static final double LOAD_FACTOR_BOUND = 0.75;
	private static final Object TOMBSTONE = new Object();

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
		contamination = 0;
		fingerprint = 0;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean put(List<Object> row) {
		row = sanitizeRow(row);
		Object key = row.get(getPrimaryIndex());
		final int hash = hashFunction1(key);

		int t = -1;
		for (int j = 0; j < array.length; j++) {
			int i = wrap(hash + (j % 2 == 0 ? j * j : -j * j));

			if (array[i] == TOMBSTONE) {
				// if t has never been assigned an index
				if (t == -1) {
					// then update t to match
					t = i;
				}
				continue;

			}

			if (array[i] == null) { // miss
				if (t == -1) {
					// if t has never been assigned an index
					//	 store the row at position i
					array[i] = row;
				}
				// otherwise, t is a recycling location
				else {
					// store the row at position t
					array[t] = row;
					// adjust the metadata
					//	 contamination decreases by 1
					contamination--;

				}
				// adjust the metadata
				//	 size increases by 1 (common)
				//	 fingerprint increases (common)
				size++;
				fingerprint += row.hashCode();

				if (loadFactor() > LOAD_FACTOR_BOUND)
					rehash();

				return false;
			}

			var old = (List<Object>) array[i];
			if (old.get(getPrimaryIndex()).equals(key)) {//hit
				//if t has never been assigned an index
				if (t == -1) {
					//		update the row at position i with the new row
					array[i] = row;
				}

				// otherwise, t is a recycling location
				else {
					//		replace the tombstone at position t with the row
					array[t] = row;
					//		replace the old row at position i with a tombstone
					array[i] = TOMBSTONE;
				}
				// adjust the metadata
				//	 fingerprint increases/decreases (common)
				fingerprint += row.hashCode() - old.hashCode();
				return true;
			}
			// NOT YET A MISS OR HIT, SO CONTINUE
		}
		throw new IllegalStateException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean remove(Object key) {
		final int hash = hashFunction1(key);

		//think of list<Object> as row
		for (int j = 0; j < array.length; j++) {
			int i = wrap(hash + (j % 2 == 0 ? j * j : -j * j));

			if (array[i] == TOMBSTONE)
				continue;

			if (array[i] == null)  // miss
				return false;

			var old = (List<Object>) array[i];
			if (old.get(getPrimaryIndex()).equals(key)) { // HIT
				// replace the old row at position i with a tombstone
				array[i] = TOMBSTONE;
				// adjust the metadata
				//		size decreases by 1
				//		contamination increases by one
				//		fingerprint decreases (common)
				size--;
				contamination++;
				fingerprint -= old.hashCode();
				return true;
			}
			// NOT YET A MISS OR HIT, SO CONTINUE
		}
		throw new IllegalStateException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Object> get(Object key) {
		final int hash = hashFunction1(key);

		//think of list<Object> as row
		for (int j = 0; j < array.length; j++) {
			int i = wrap(hash + (j % 2 == 0 ? j * j : -j * j));

			if (array[i] == TOMBSTONE)
				continue;

			if (array[i] == null)  // miss
				return null;

			var old = (List<Object>) array[i];
			if (old.get(getPrimaryIndex()).equals(key)) { // HIT
				return old;
			}
			// NOT YET A MISS OR HIT, SO CONTINUE
		}
		throw new IllegalStateException();
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public int capacity() {
		return array.length;
	}

	@Override
	public double loadFactor() {
		return (double) (size + contamination) / (double) array.length;
	}

	@SuppressWarnings("unchecked")
	public void rehash() {

		// let backup reference point to existing array
		var backup = array;

		// reassign array reference to point to
		// 		a new array which is roughly twice as large 
		//		but still a valid prime number (based on CRT)
		//		by find nextPrime from current capacity
		array = new Object[nextPrime(backup.length)];

		// reset all metadata
		size = 0;
		contamination = 0;
		fingerprint = 0;

		// for each row (not a null or tombstone)in the table:
		//		call put with that row
		for (int j = 0; j < backup.length; j++) {
			if (backup[j] != null && backup[j] != TOMBSTONE) {
				put((List<Object>) backup[j]);
			}
		}
	}

	private int nextPrime(int prev) {
		// let next prime number (represented by next variable) be twice the prev plus 1
		int next = (prev * 2) + 1;
		// ASQP or in general:
		// if not the case that next modulo 4 is congruent to 3
		if (!(next % 4 == 3)) {
			// step next up by 2
			next += 2;
		}
		// while it's not the case that next isPrime:
		while (!isPrime(next)) {
			//step next up by 4
			next += 4;
		}

		return next;
	}

	private boolean isPrime(int number) {
		double sqrt = Math.sqrt(number);
		// for each factor of the number: from 3 up to and including the square root (compute before loop) stepping up by 2 each time
		for (int i = 3; i <= sqrt; i += 2) {
			//if number is divisible by the factor:
			if (number % i == 0) {
				//return false
				return false;
			}
		}
		//return true
		return true;

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
		return new Iterator<>() {

			//initialize loop control
			int index = skip(0); // first valid index

			@Override
			public boolean hasNext() { // Maintenance condition
				// answer the question index valid? if so, yes. otherwise, no.
				return index < array.length;
			}

			@SuppressWarnings("unchecked")
			@Override
			public List<Object> next() { // body, incrementation
				// answer the question what is the next thing
				//temp copy of row
				var temp = (List<Object>) array[index];
				//set index to skip(index+1)
				index = skip(index + 1);
				//return the row
				return temp;

			}

			private int skip(int i) { // OA
				//check to make sure not going outside of bounds
				if (i < array.length) {
					//finds the next actual row after i (if i is invalid)
					while ((i < array.length) && (array[i] == null || array[i] == TOMBSTONE)) {
						i++;
					}
				}
				return i;
			}

		};
	}
}
