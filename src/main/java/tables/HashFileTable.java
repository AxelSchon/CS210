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
			throw new RuntimeException("Schema/metaData file I/O error", e);
		}

		setTableName(tableName);
		setColumnNames(columnNames);
		setColumnTypes(columnTypes);
		setPrimaryIndex(primaryIndex);

		writeSchema(); // write schema information

		recordWidth(); // calculate maximum record width

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

		try {
			// ensure that the schema and metadata files exist. If not throw exception	
			if (Files.notExists(root.resolve("schema")) || Files.notExists(root.resolve("schema")))
				throw new IOException("Missing Schema or MetaData file");

			// assign the file channels for both schema and metadata
			schema = FileChannel.open(root.resolve("schema"), READ, WRITE);
			metadata = FileChannel.open(root.resolve("metadata"), READ, WRITE);
		} catch (IOException e) {
			throw new RuntimeException("Schema/MetaData file I/O error", e);
		}

		setTableName(tableName); // content of schema doesn't include name of the table
		readSchema(); // set column name/type/primary index
		readMetaData(); // set metaData (size, fingerprint, recordWidth). This method brings record width back from the file.  
	}

	@Override
	public boolean put(List<Object> row) {
		row = sanitizeRow(row);

		var key = row.get(getPrimaryIndex());
		var digest = digest(key);

		// check to make sure that what comes back from the put method is the correct row
		var old = readRecord(digest); // old record. To identify hit/miss
		System.out.println(old);

		writeRecord(digest, row); // write record for the digest of row

		if (old == null) { // miss
			// update metaData for miss
			size++;
			fingerprint += row.hashCode();
			// write update into file
			writeMetaData();

			return false;
		} else { // hit
			//update metaData for hit
			fingerprint += row.hashCode() - old.hashCode();
			// write update into file
			writeMetaData();

			return true;
		}
	}

	@Override
	public boolean remove(Object key) {
		// get digest for the key
		var digest = digest(key);
		// find the existing row (if any) for that digest
		var old = readRecord(digest);

		if (old == null) { // miss  
			return false; // terminate/return false

		} else { // hit

			try {// delete the corresponding file
				if (Files.exists(pathOf(digest))) {
					if (Files.exists(pathOf(digest))) {
						Files.walk(pathOf(digest)).sorted(Comparator.reverseOrder()).map(path -> path.toFile())
								.forEach(file -> file.delete());
					}
				}
				// update metaData for hit
				size--;
				fingerprint -= old.hashCode();
				// write update into file
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

	// recursive delete operation. Wipes out state. 
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
		truncate(); // wipe out content of the state

		// reset the metadata
		size = 0;
		fingerprint = 0;

		writeMetaData(); // update file storage with changes
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
			buf.putInt(size); // put into buffer int representing size
			buf.putInt(fingerprint); // put into buffer int representing fingerprint
			buf.putInt(recordWidth); // put into buffer int representing recordWidth
		} catch (IOException e) {
			throw new RuntimeException("Metadata file I/O error", e);
		}
	}

	public void readMetaData() {
		try {
			// READ METADATA
			var buf = metadata.map(READ_ONLY, 0, MAX_INT_SIZE * 3);
			size = (buf.getInt()); // get from buffer int representing size
			fingerprint = (buf.getInt()); // get from buffer int representing fingerprint
			recordWidth = (buf.getInt()); // get from buffer int representing recordWidth
		} catch (IOException e) {
			throw new RuntimeException("Metadata file I/O error", e);
		}
	}

	public void writeSchema() {
		try {
			// WRITE SCHEMA
			var buf = schema.map(READ_WRITE, 0,
					MAX_INT_SIZE * 2 + (LENGTH_NAME_BYTE + MAX_COL_NAME_LENGTH + LENGTH_TYPE_BYTE) * MAX_COL_COUNT);

			buf.putInt(getColumnNames().size()); // put into buffer the number of columns
			buf.putInt(getPrimaryIndex()); // put into buffer the primary index

			for (int i = 0; i < getColumnNames().size(); i++) { // for the number of columns 
				var name = getColumnNames().get(i); // name of column
				var chars = name.getBytes(UTF_8); // characters the name is composed of
				buf.put((byte) chars.length); // put into buffer the length of the byte array
				buf.put(chars); // put bytes of the character array
				buf.put(new byte[15 - chars.length]); // fill in padding by creating byte array of size (max size) - (chars.length)

				var type = getColumnTypes().get(i); // type of column 
				buf.put((byte) type.getTypeNumber()); // put the type number (1->String, 2->integer, 3->boolean)	

			}
		} catch (IOException e) {
			throw new RuntimeException("Schema file I/O error", e);
		}
	}

	public void readSchema() {
		var colNames = new ArrayList<String>();
		var colTypes = new ArrayList<FieldType>();

		try {
			var buf = schema.map(READ_WRITE, 0,
					MAX_INT_SIZE * 2 + (LENGTH_NAME_BYTE + MAX_COL_NAME_LENGTH + LENGTH_TYPE_BYTE) * MAX_COL_COUNT);

			size = buf.getInt(); // set column count
			setPrimaryIndex(buf.getInt()); // set primary index

			for (int i = 0; i < getColumnTypes().size(); i++) {
				var length = buf.get(); // get length of byte array
				var chars = new byte[length]; // create byte array of required size
				buf.get(chars); // fill up newly created byte array
				colNames.add(new String(chars, UTF_8)); // create string from byte array and add to colNames
				buf.get(new byte[MAX_COL_NAME_LENGTH - chars.length]); // fill up byte array with size equal to amount of padding left over
				colTypes.add(i, FieldType.valueOf(buf.get())); // get field type number and convert: (1->String, 2->Integer, 3->Boolean)
			}
			setColumnNames(colNames); // set column names
			setColumnTypes(colTypes); // set column types
		} catch (IOException e) {
			throw new RuntimeException("Schema file I/O error", e);
		}
	}

	private void writeRecord(String digest, List<Object> row) {
		try {
			Path file = pathOf(digest); // define path based on digest
			Files.createDirectories(file.getParent()); // create any missing folders along file path

			var channel = FileChannel.open(file, CREATE, READ, WRITE); // open given path. Create new file if it doesn't exist or reopen if it does. 
			var buf = channel.map(READ_WRITE, 0, recordWidth); // map for size of the variable width record

			for (int i = 0; i < getColumnTypes().size(); i++) { // for number of columns
				var columnType = getColumnTypes().get(i); // save the column type
				var element = row.get(i); // save element at index in row

				if (columnType == FieldType.STRING) {
					if (element != null) {
						var str = element.toString(); // String in row
						var chars = str.getBytes(UTF_8); // characters the string is composed of
						buf.put((byte) chars.length); // puts into the buffer the length of the byte array
						buf.put(chars); // put bytes of the character array (write out characters themselves)
					} else
						buf.put((byte) -1); // if null, put -1

				} else if (columnType == FieldType.INTEGER) {
					if (element != null) {
						if ((int) element <= 127) { // if element is an Integer which can be stored in one byte:
							buf.put((byte) Byte.BYTES); // set prefix to 1
							buf.put(((Integer) element).byteValue()); // store element as a byte
						} else if ((int) element <= 32767) { // if element is an Integer which is larger than one byte but can be stored in two bytes:
							buf.put((byte) Short.BYTES); // set prefix to 2
							buf.putShort(((Integer) element).shortValue()); // store element as a short
						} else if ((int) element <= 200000000) { // if element is an Integer which can not be stored in two bytes:
							buf.put((byte) Integer.BYTES); // set prefix to 4
							buf.putInt(((Integer) element).intValue()); // store element as an int
						}
					} else
						buf.put((byte) -1); // if null, put -1

				} else if (columnType == FieldType.BOOLEAN) {
					if (element == null) {
						buf.put((byte) -1); // if null, put 1
					} else if ((boolean) element == true) { // if element is true:
						buf.put((byte) 1); // put 1
					} else if ((boolean) element == false) { // if element is false:
						buf.put((byte) 0); // put 0
					}
				}
			}
		} catch (IOException e) {
			throw new RuntimeException("State file I/O error", e);
		}
	}

	private List<Object> readRecord(String digest) {
		var record = new ArrayList<Object>(); // create ArrayList to store row
		try {
			Path file = pathOf(digest);// define path based on digest
			Files.createDirectories(file.getParent());// create any missing folders along file path

			// if the file doesn't exist, terminate/return null
			if (Files.notExists(file))
				return null; //miss

			var channel = FileChannel.open(file, READ); // open given path. Create new file if it doesn't exist or reopen if it does.
			var buf = channel.map(READ_ONLY, 0, recordWidth); // // map for size of the variable width record

			for (int i = 0; i < getColumnNames().size(); i++) { // for number of columns
				var columnType = getColumnTypes().get(i); // save the column type

				if (columnType == FieldType.STRING) { // if columnType is a string:
					var len = buf.get(); // get length of string
					if (len == -1) // if length of string is null:
						record.add(null); // add null to the record
					else { // otherwise
						var chars = new byte[len]; // create new byte array of size len
						buf.get(chars); // fill len amount of characters into newly created byte array
						record.add(new String(chars, UTF_8)); // create string from byte array and add to record
					}
				} else if (columnType == FieldType.INTEGER) { // if column type is an Integer:
					var width = buf.get(); // get Integer field prefix 
					if (width == 4) // if prefix determines Integer is an int
						record.add(buf.getInt()); // read in 4 bytes
					else if (width == 2) // if prefix determines Integer is a short
						record.add((int) buf.getShort()); // read in 2 bytes
					else if (width == 1) // if prefix determines Integer is a byte
						record.add((int) buf.get()); // read in 1 byte
					else
						record.add(null); // else prefix is -1. read in null
				} else if (columnType == FieldType.BOOLEAN) { // if columnType is a Boolean:
					var val = buf.get(); // get boolean byte
					if (val == 0) // if val is 0:
						record.add(false); // read in false
					else if (val == 1) // if val is 1:
						record.add(true); // read in true
					else
						record.add(null); // else boolean is -1. read in null
				}
			}
			System.out.println(record);
			return record; // hit
		} catch (IOException e) {
			throw new RuntimeException("State file I/O error", e);
		}
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
		// "I have this many fields of this type, need to account for this many bytes stored in the file"
		for (int i = 0; i < getColumnTypes().size(); i++) {
			var columnType = getColumnTypes().get(i);
			if (columnType == FieldType.STRING) {
				recordWidth += 1 + 127; // String prefix plus max string width
			} else if (columnType == FieldType.INTEGER) {
				recordWidth += 1 + 4; // Integer prefix plus max Integer width
			} else if (columnType == FieldType.BOOLEAN) {
				recordWidth += 1; // Boolean width
			}
		}
	}

	@Override
	public Iterator<List<Object>> iterator() {
		// resolve and walk state folder:
		var state = root.resolve("state");

		try {
			return Files.walk(state).filter(path -> !Files.isDirectory(path)).map(path -> digestOf(path))
					.map(digest -> readRecord(digest)).iterator();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
