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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;

import sql.FieldType;

/**
 * Implements a hash-based table
 * using a directory tree structure.
 */
public class HashFileTable extends PrettyTable {
	private Path root;
	private FileChannel schema, metadata;

	// fields for metadata (size, fingerprint) 
	private int size;
	private int fingerprint;
	private int recordWidth;

	// define constants for the limits of the data types
	private static final int MAX_STRING_LENGTH = 127; // remember to keep track of length (1 byte)
	private static final int MAX_INT_SIZE = 4; // size of int
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
		try {
			// assign the root path based on the table name
			root = Paths.get("data", "tables", tableName);
			// if there is already a root, walk to recursively delete it (suggest delete helper method)
			if (Files.exists(root)) {
				//replace with delete()?;
				if (Files.exists(root)) {
					Files.walk(root).sorted(Comparator.reverseOrder()).map(path -> path.toFile())
							.forEach(file -> file.delete());
				}
			}
			// create any missing dirs for root path
			Files.createDirectories(root);
			// assign the file channels for both schema and metadata
			schema = FileChannel.open(root.resolve("schema"), CREATE_NEW, READ, WRITE);
			metadata = FileChannel.open(root.resolve("metadata"), CREATE_NEW, READ, WRITE);
		} catch (IOException e) {
			throw new RuntimeException("Schema/mMtaData file I/O error", e);
		}

		setTableName(tableName);
		setColumnNames(columnNames);
		setColumnTypes(columnTypes);
		setPrimaryIndex(primaryIndex);

		// writeSchema(); // based on the example
		writeSchema();

		recordWidth();

		clear(); // which calls writeMetaData();
	}

	/**
	 * Reopens a table from an
	 * existing file structure.
	 *
	 * @param tableName a table name
	 */
	public HashFileTable(String tableName) {
		// assign the root based on the table name
		root = Paths.get("data", "tables", tableName);
		// ensure that the schema and metadata files exist. If not throw exception	
		if (Files.notExists(root.resolve("schema")) || Files.notExists(root.resolve("schema")))
			throw new RuntimeException("Missing Schema or MetaData file");
		// assign the file channels for both schema and metadata
		try {
			schema = FileChannel.open(root.resolve("schema"), READ, WRITE);
			metadata = FileChannel.open(root.resolve("metadata"), READ, WRITE);
		} catch (IOException e) {
			throw new RuntimeException("Schema/MetaData file I/O error", e);
		}

		setTableName(tableName);
		// readSchema(); // based on example
		readSchema();

		// readMethadata(); // based on the example (record width should come from this since I store it as metadata)
		readMetaData();
	}

	@Override
	public boolean put(List<Object> row) {
		row = sanitizeRow(row);

		var key = row.get(getPrimaryIndex());
		var digest = digest(key);

		// check to make sure that what comes back from the put method is the correct row
		var old = readRecord(digest); // identify hit/miss
		System.out.println(old);
		writeRecord(digest, row);

		if (old == null) { // miss
			size++;
			fingerprint += row.hashCode();
			writeMetaData();
			return false;
		} else { // hit
			fingerprint += row.hashCode() - old.hashCode();
			writeMetaData();
			return true;
		}
	}

	@Override
	public boolean remove(Object key) {
		// get digest for the key
		var digest = digest(key);

		// find the existing row (if any) for that digest
		var row = readRecord(digest);
		// if it is a miss, terminate method (return false)
		if (row == null) { // miss  
			return false;
		} else { // hit
			// delete the corresponding file
			try {
				if (Files.exists(pathOf(digest))) {
					//replace with delete()?;
					if (Files.exists(pathOf(digest))) {
						Files.walk(pathOf(digest)).sorted(Comparator.reverseOrder()).map(path -> path.toFile())
								.forEach(file -> file.delete());
					}
				}
				size--;
				fingerprint -= row.hashCode();
				writeMetaData();
				return true;
			} catch (IOException e) {
				throw new RuntimeException("Could not remove file", e);
			}
		}
	}

	@Override
	public List<Object> get(Object key) {
		var digest = digest(key);
		return readRecord(digest);
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public int capacity() {
		return size;
	}

	//wipes out state
	private void truncate() {
		// resolve the state dir
		var state = root.resolve("state");
		// if state dir  is not there yet, just quit this method
		if (Files.exists(state)) {
			;
		}
		// if there is, recursively delete the state folder using a walk (as in the example)
		else {
			try {
				if (Files.exists(state)) {
					Files.walk(state).sorted(Comparator.reverseOrder()).map(path -> path.toFile())
							.forEach(file -> file.delete());
				}
			} catch (IOException e) {
				throw new RuntimeException("State file I/O error", e);
			}
		}
	}

	@Override
	public void clear() {
		// empty all rows from table
		truncate();

		// reset the metadata
		size = 0;
		fingerprint = 0;

		// writeMetadata to file;
		writeMetaData();
	}

// optional for M2
	private void delete() {
		truncate();
		try {
			// delete schema file
			Files.delete(root.resolve("schema"));
			// delete metadata file
			Files.delete(root.resolve("metadata"));
			// delete root folder
			Files.delete(root);
		} catch (IOException e) {
			throw new RuntimeException("Error deleting Metadata and/or Schema", e);
		}
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

// convert from digests string to corresponding path 
// path 12345678abcdef -> "12/345678abcdef"
	private Path pathOf(String digest) {
		// get the 2 prefix chars
		String prefix = digest.substring(0, 2);

		// get the 38 suffix chars
		String suffix = digest.substring(2);

		// return a path from the root
		// which resolves the prefix
		// and then resolves the suffix
		return root.resolve("state").resolve(prefix).resolve(suffix);
	}

// path 12/345678abcdef -> "12345678abcdef"
	private String digestOf(Path path) {
		StringBuilder sb = new StringBuilder();

		// get the folder name (asking the path object for its parent's filename as a string)
		String folderName = path.getParent().getFileName().toString();

		// get the file name (asking the path object for its filename)
		String fileName = path.getFileName().toString();

		// concatenate folder and file names together
		sb.append(folderName).append(fileName);
		String digestOf = sb.toString();

		//return concatenated names
		return digestOf;
	}

	public void deleteMetaData() {
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
		try {
			// WRITE METADATA
			var buf = metadata.map(READ_WRITE, 0, MAX_INT_SIZE * 3);
			buf.putInt(size);
			buf.putInt(fingerprint);
			buf.putInt(recordWidth);
		} catch (IOException e) {
			throw new RuntimeException("Metadata file I/O error", e);
		}
	}

	public void readMetaData() {
		try {
			// READ METADATA
			var buf = metadata.map(READ_ONLY, 0, MAX_INT_SIZE * 3);
			size = (buf.getInt());
			fingerprint = (buf.getInt());
			recordWidth = (buf.getInt());
		} catch (IOException e) {
			throw new RuntimeException("Metadata file I/O error", e);
		}
	}

	public void writeSchema() {
		try {
			// WRITE SCHEMA
			var buf = schema.map(READ_WRITE, 0,
					MAX_INT_SIZE * 2 + (LENGTH_NAME_BYTE + MAX_COL_NAME_LENGTH + LENGTH_TYPE_BYTE) * MAX_COL_COUNT);

			buf.putInt(getColumnNames().size());
			buf.putInt(getPrimaryIndex());

			for (int i = 0; i < getColumnNames().size(); i++) {
				var name = getColumnNames().get(i);
				var chars = name.getBytes(UTF_8);
				buf.put((byte) chars.length);
				buf.put(chars);
				buf.put(new byte[15 - chars.length]);

				var type = getColumnTypes().get(i);
				buf.put((byte) type.getTypeNumber());

			}
		} catch (IOException e) {
			throw new RuntimeException("Schema file I/O error", e);
		}
	}

	public void readSchema() {
		var colNames = new ArrayList<String>();
		var colTypes = new ArrayList<FieldType>();

		try {
			// REOPEN EXISTING TREE
			if (Files.notExists(root.resolve("schema")))
				throw new IOException("Missing schema file");
			schema = FileChannel.open(root.resolve("schema"), READ, WRITE);

			// READ SCHEMA
			{
				var buf = schema.map(READ_WRITE, 0,
						MAX_INT_SIZE * 2 + (LENGTH_NAME_BYTE + MAX_COL_NAME_LENGTH + LENGTH_TYPE_BYTE) * MAX_COL_COUNT);

				size = buf.getInt();
				setPrimaryIndex(buf.getInt());

				for (int i = 0; i < getColumnNames().size(); i++) {
					var length = buf.get();
					var chars = new byte[length];
					buf.get(chars);
					colNames.add(new String(chars, UTF_8));
					buf.get(new byte[MAX_COL_NAME_LENGTH - chars.length]);
					colTypes.add(i, FieldType.valueOf(buf.get()));
				}
			}
			setColumnNames(colNames);
			setColumnTypes(colTypes);
		} catch (IOException e) {
			throw new RuntimeException("Schema file I/O error", e);
		}
	}

	private void writeRecord(String digest, List<Object> row) {
		//Path to folder containing files: (1st two digest chars)
		Path fileFolder = root.resolve("state").resolve(digest.substring(0, 2));

		try {
			// CREATE NEW TREE
			Files.createDirectories(fileFolder);
			// path to files within folder. (last 38 digest chars)
			Path file = fileFolder.resolve(digest.substring(2));

			// WRITE STATE
			var channel = FileChannel.open(file, CREATE, READ, WRITE);
			var buf = channel.map(READ_WRITE, 0, recordWidth);

			// for the number of columns
			for (int i = 0; i < getColumnNames().size(); i++) {
				// save that columns type
				var columnType = getColumnTypes().get(i);
				// save element at index in row
				var element = row.get(i);

				if (columnType == FieldType.STRING) {
					if (element != null) {
						var str = element.toString();
						var chars = str.getBytes(UTF_8);
						buf.put(chars);
					} else
						buf.put((byte) -1);
				} else if (columnType == FieldType.INTEGER) { // need if statement for	
					buf.put((byte) Short.BYTES);
					buf.putShort((short) 1);
				} else if (columnType == FieldType.BOOLEAN) {
					if ((boolean) element == true) {
						buf.put((byte) 1);
					} else if ((boolean) element == false) {
						buf.put((byte) 0);
					} else {
						buf.put((byte) -1);
					}
				}
			}
		} catch (IOException e) {
			throw new RuntimeException("State file I/O error", e);
		}
	}

	private List<Object> readRecord(String digest) {
		// DUMMY FIELDS
		Path fileFolder = root.resolve("state").resolve(digest.substring(0, 2));

		var record = new ArrayList<Object>();

		try {
			// CREATE NEW TREE
			Files.createDirectories(fileFolder);
			// path to files within folder. (last 38 digest chars)
			Path file = fileFolder.resolve(digest.substring(2));

			// DEFINE PATH FROM DIGEST
			//var path = pathOf(digest);

			// REOPEN EXISTING TREE
			if (Files.notExists(file))
				return null;
			// READ STATE
			{
				var channel = FileChannel.open(file, READ);
				var buf = channel.map(READ_ONLY, 0, recordWidth);

				for (int i = 0; i < getColumnNames().size(); i++) {
					// save that columns type
					var columnType = getColumnTypes().get(i);

					if (columnType == FieldType.STRING) {
						var len = buf.get();
						if (len == -1)
							record.add(null);
						else {
							var chars = new byte[len];
							buf.get(chars);
							record.add(new String(chars, UTF_8));
						}
					} else if (columnType == FieldType.INTEGER) {
						var bytes = buf.get();
						if (bytes == 4)
							record.add(buf.getInt());
						else if (bytes == 2)
							record.add((int) buf.getShort());
						else if (bytes == 1)
							record.add((int) buf.get());
						else
							record.add(null);
					} else if (columnType == FieldType.BOOLEAN) {
						var val = buf.get();
						if (val == 0)
							record.add(false);
						else if (val == 1)
							record.add(true);
						else
							record.add(null);
					}
				}
				System.out.println(record);
				return record;
				//ensure called with valid row
			}
		} catch (IOException e) {
			throw new RuntimeException("State file I/O error", e);
		}

		// return a row on a hit
		// or else a null on a miss
	}

	@Override
	public int hashCode() {
		return fingerprint;
	}

	// measure the maximum width of a record
	//	 need to know columnTypes() to perform
	//	 looping through each columnTyype and asking is it a string? then add this many bytes to record width etc.
	public void recordWidth() {
		recordWidth = 0;
		for (int i = 0; i < getColumnTypes().size(); i++) {
			var columnType = getColumnTypes().get(i);
			if (columnType == FieldType.STRING) {
				recordWidth += 1 + 127;
			} else if (columnType == FieldType.INTEGER) {
				recordWidth += 1 + 4;
			} else if (columnType == FieldType.BOOLEAN) {
				recordWidth += 1;
			}
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
