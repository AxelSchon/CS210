package tables;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;

import sql.FieldType;

/**
 * Implements a hash-based table
 * using a directory tree structure.
 */
public class HashFileTable extends Table {
	private Path root;
	private FileChannel schema, metadata;
	// field: maximum record width (in his solution: calculate during constructor and store in field) (can be metadata, if you want... harder?)

	// fields for metadata (size, fingerprint) 
	private int size;
	private int fingerprint;
	private int recordWidth;
	// define constants for the limits of the data types
	private static final int MAX_STRING_LENGTH = 127; // remember to keep track of length (1 byte)
	private static final int INT_SIZE = 4; // size of int
	private static final int BOOLEAN_SIZE = 1; // 
	private static final int MAX_COL_NAME_LENGTH = 15; // max length of name
	private static final int MAX_COL_COUNT = 15; // max number of columns
	private static final int LENGTH_NAME_BYTE = 1; // byte before name which states length of name
	private static final int LENGTH_TYPE_BYTE = 1; // byte which states type
	private static final int LENGTH_INT_BYTE = 1; //  byte which states how many bytes field 2 stores within state

	// eg. max string length, max column name length, max column count,  etc 

	/**
	 * Creates a table and initializes
	 * the file structure.
	 *
	 * @param tableName    a table name
	 * @param columnNames  the column names
	 * @param columnTypes  the column types
	 * @param primaryIndex the primary index
	 */
	public HashFileTable(String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex) {
		// follow example code to do the following:
		// assign the root path based on the table name
		Path root = Paths.get("data", "tables", tableName);
		// if there is already a root, walk to recursively delete it (suggest delete helper method)
		deleteMetaData();
		// create any missing dirs for root path
		// assign the file channels for both schema and metadata

		setTableName(tableName);
		setColumnNames(columnNames);
		setColumnTypes(columnTypes);
		setPrimaryIndex(primaryIndex);
		// writeSchema(); // based on the example
		// measure the maximum width of a record

		clear(); // which calls writeMetaData();
	}

	/**
	 * Reopens a table from an
	 * existing file structure.
	 *
	 * @param tableName a table name
	 */
	public HashFileTable(String tableName) {
		// follow example code to do the following:
		// assign the root based on the table name
		// ensure that the schema and metadata files exist. If not throw exception	
		// assign the file channels for both schema and metadata

		setTableName(tableName);
		// readSchema(); // based on example
		// measure the maximum record width

		// readMethadata(); // based on the example
	}

	@Override
	public void clear() {
		truncate(); // empty all rows from table
		// reset the metadata
		// writeMetadata();
	}

	private void truncate() {
		// resolve the state dir
		// if state dir  is not there yet, just quit this method
		// if there is, recursively delete the state folder using a walk (as in the example)
	}

// optional for M2
	private void delete() {
		truncate();
		// delete metadata file
		// delete schema file
		// delete root folder
	}

	@Override
	public boolean put(List<Object> row) {
		row = sanitizeRow(row);

		var key = row.get(getPrimaryIndex());
		var digest = digest(key);

		var old = readRecord(digest);

		writeRecord(digest, row);

		// update the metadata based on hit/miss

		return false; // based on hit/miss
	}

	@Override
	public boolean remove(Object key) {
		// get digest for the key

		// find the existing row (if any) for that digest
		// if it is a miss, terminate method (return false)

		// if we make it here its a hit
		// delete the corresponding file
		// update the metadata based on hit/miss

		return true;

	}

	@Override
	public List<Object> get(Object key) {
		var digest = digest(key);

		return readRecord(digest);
	}

	private void writeRecord(String digset, List<Object> row) {
		//ensure called with valid row
		// fill in from example 3 code
	}

	private List<Object> readRecord(String digest) {
		// fill in from example code

		// return a row on a hit
		// or else a null on a miss
		return null;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public int capacity() {
		return size;
	}

	private String digest(Object key) {
		try {
			var sha1 = MessageDigest.getInstance("SHA-1");
			// "update" data into it as many times as necessary. Then "digest" built data. 
			sha1.update("saltOfChoice".getBytes(StandardCharsets.UTF_8));
			sha1.update(key.toString().getBytes(StandardCharsets.UTF_8));

			var digest = sha1.digest();
			var hex = HexFormat.of().withLowerCase();
			return hex.formatHex(digest);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("Digest error", e);
		}
	}

// path 12345678abcdef -> "12/345678abcdef"
	private Path pathOf(String digest) {
		// get the 2 prefix chars
		String prefix = digest.substring(0, 3);
		// get the 38 suffix chars
		String suffix = digest.substring(3, 39);
		// return a path from the root
		// which resolves the prefix
		// and then resolves the suffix

		//return resolved path
		return null;
	}

// path 12/345678abcdef -> "12345678abcdef"
	private String digestOf(Path path) {
		// consult the SHA-1 algorithm to produce a digest

		// convert the numeric digest to a hex string
		// get the folder name (asking the path object for its parent's filename as a string)
		// get the file name (asking the path object for its filename)
		// concatenate folder and file names together

		//return concatenated names
		return null;
	}

	public void deleteMetaData() {
		Path root = Paths.get("data", "tables", getTableName());
		try {
			// DELETE EXISTING TREE
			if (Files.exists(root)) {
				Files.walk(root).sorted(Comparator.reverseOrder()).map(path -> path.toFile())
						.forEach(file -> file.delete());
			}
		} catch (IOException e) {
			throw new RuntimeException("Metadata file I/O error", e);
		}
	}

	public void writeMetaData() {
		// DUMMY FIELDS
		Path root = Paths.get("data", "tables", getTableName());
		FileChannel metadata;

		try {
			// CREATE NEW TREE
			Files.createDirectories(root);
			metadata = FileChannel.open(root.resolve("metadata"), CREATE_NEW, READ, WRITE);

			// WRITE METADATA
			{
				var buf = metadata.map(READ_WRITE, 0, INT_SIZE * 3);
				buf.putInt(size);
				buf.putInt(fingerprint);
				buf.putInt(recordWidth);
			}
		} catch (IOException e) {
			throw new RuntimeException("Metadata file I/O error", e);
		}
	}

	public void writeSchema() {
		// DUMMY FIELDS
		Path root = Paths.get("data", "tables", getTableName());
		FileChannel schema;
		try {
			// CREATE NEW TREE
			Files.createDirectories(root);
			schema = FileChannel.open(root.resolve("schema"), CREATE_NEW, READ, WRITE);

			// WRITE SCHEMA
			var buf = schema.map(READ_WRITE, 0,
					INT_SIZE * 2 + (LENGTH_NAME_BYTE + MAX_COL_NAME_LENGTH + LENGTH_TYPE_BYTE) * MAX_COL_COUNT);

			buf.putInt(getColumnNames().size());
			buf.putInt(getPrimaryIndex());

			for (int i = 0; i < getColumnNames().size(); i++) {
				var name = getColumnNames().get(i);
				var chars = name.getBytes(UTF_8);
				buf.put((byte) chars.length);
				buf.put(chars);
				buf.put(new byte[15 - chars.length]);

				var type = getColumnTypes().get(i);
				;
				buf.put((byte) type.getTypeNumber());

			}
		} catch (IOException e) {
			throw new RuntimeException("Schema file I/O error", e);
		}
	}

	public void writeState() {
		// DUMMY FIELDS
		Path root = Paths.get("data", "tables", getTableName());
		// DEFINE PATH FROM DIGEST

		try {
			var path = root.resolve("state").resolve("a1").resolve("b2c3");

			// CREATE NEW TREE
			Files.createDirectories(path.getParent());

			// WRITE STATE
			{
				var channel = FileChannel.open(path, CREATE, READ, WRITE);
				var buf = channel.map(READ_WRITE, 0,
						LENGTH_NAME_BYTE + MAX_STRING_LENGTH + LENGTH_INT_BYTE + INT_SIZE + BOOLEAN_SIZE);

				// DUMMY FIELD 0 STRING
				// WIDTH: 1 + max string length
				{
					var str = "alpha"; // DUMMY LETTER VALUE
					var chars = str.getBytes(UTF_8);
					buf.put((byte) chars.length);
					buf.put(chars);
				}

				// DUMMY FIELD 1 INTEGER-AS-SHORT
				// WIDTH: 1 + 2
				{
					buf.put((byte) Short.BYTES);
					buf.putShort((short) 1); // DUMMY ORDER VALUE
				}

				// DUMMY FIELD 2 BOOLEAN
				// WIDTH: 1
				{
					buf.put((byte) 1); // DUMMY VOWEL VALUE
				}
			}
		} catch (IOException e) {
			throw new RuntimeException("State file I/O error", e);
		}
	}

	public void readMetaData() {

		Path root = Paths.get("data", "tables", getTableName());
		FileChannel metadata;

		try {

			// REOPEN EXISTING TREE
			if (Files.notExists(root.resolve("metadata")))
				throw new IOException("Missing metadata file");
			metadata = FileChannel.open(root.resolve("metadata"), READ);

			// READ METADATA
			{
				var buf = metadata.map(READ_ONLY, 0, INT_SIZE * 3);

				size = (buf.getInt());
				fingerprint = (buf.getInt());
				recordWidth = (buf.getInt());
			}

		} catch (IOException e) {
			throw new RuntimeException("Metadata file I/O error", e);
		}
	}

	public void readSchema() {
		// DUMMY FIELDS
		Path root = Paths.get("data", "tables", getTableName());
		FileChannel schema;

		try {
			// REOPEN EXISTING TREE
			if (Files.notExists(root.resolve("schema")))
				throw new IOException("Missing schema file");
			schema = FileChannel.open(root.resolve("schema"), READ, WRITE);

			// READ SCHEMA
			{
				var buf = schema.map(READ_WRITE, 0,
						INT_SIZE * 2 + (LENGTH_NAME_BYTE + MAX_COL_NAME_LENGTH + LENGTH_TYPE_BYTE) * MAX_COL_COUNT);

				System.out.println(buf.getInt());
				System.out.println(buf.getInt());

				for (int i = 0; i < getColumnNames().size(); i++) {
					var length = buf.get();
					var chars = new byte[length];
					buf.get(chars);
					System.out.println(new String(chars, UTF_8));
					buf.get(new byte[MAX_COL_NAME_LENGTH - chars.length]);
					System.out.println(FieldType.valueOf(buf.get()));
				}
			}
		} catch (IOException e) {
			throw new RuntimeException("Schema file I/O error", e);
		}
	}

	public void readState() {
		// DUMMY FIELDS
		Path root = Paths.get("data", "tables", getTableName());

		try {
			// DEFINE PATH FROM DIGEST
			var path = root.resolve("state").resolve("a1").resolve("b2c3");
			// REOPEN EXISTING TREE
			if (Files.notExists(path))
				throw new IOException("Missing state file");

			// READ STATE
			{
				var channel = FileChannel.open(path, READ);
				var buf = channel.map(READ_ONLY, 0, 1 + 127 + 1 + 4 + 1);

				// DUMMY FIELD 0 STRING
				// WIDTH: 1 + max string length
				{
					var len = buf.get();
					var chars = new byte[len];
					buf.get(chars);
					System.out.println(new String(chars, UTF_8)); // DUMMY LETTER VALUE
				}

				// DUMMY FIELD 1 INTEGER-AS-SHORT
				// WIDTH: 1 + 2
				{
					buf.get();
					System.out.println(buf.getShort()); // DUMMY ORDER VALUE
				}

				// DUMMY FIELD 2 BOOLEAN
				// WIDTH: 1
				{
					System.out.println(buf.get() == 1); // DUMMY VOWEL VALUE
				}
			}
		} catch (IOException e) {
			throw new RuntimeException("State file I/O error", e);
		}
	}

	@Override
	public int hashCode() {
		return fingerprint;
	}

	public void recordWidth() {
		for (int i = 0; i < MAX_COL_COUNT; i++) {

		}
	}

	@Override
	public Iterator<List<Object>> iterator() {
		var state = root.resolve("state");
		try {
			return Files.walk(state).filter(path -> !Files.isDirectory(path)).map(path -> digestOf(path))
					.map(digest -> readRecord(digest)).iterator();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
