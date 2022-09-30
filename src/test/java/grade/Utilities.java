package grade;

import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static sql.FieldType.*;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.function.ThrowingSupplier;

import sql.FieldType;
import tables.Table;

@TestInstance(Lifecycle.PER_CLASS)
abstract class AbstractTableContainer {
	static final Path LOGS_DIRECTORY = Paths.get("data", "logs");
	static final int TIMEOUT_MILLIS = 100;

	String tableName;
	List<String> columnNames;
	List<FieldType> columnTypes;
	int primaryIndex;

	Table subject;
	ControlTable control;
	int hits, misses, passed;
	PrintStream log;
	Random RNG;

	@BeforeAll
	void startStats() {
		hits = 0;
		misses = 0;
		passed = 0;
	}

	@BeforeAll
	void defineRNG() {
		RNG = new Random();
	}

	Table testConstructor(ThrowingSupplier<Table> supplier, List<String> exempt) {
		try {
			var table = assertTimeout(ofMillis(TIMEOUT_MILLIS * 100),
				supplier,
				"Timeout in constructor (infinite loop/recursion likely)"
			);
			logConstructor(table.getClass().getSimpleName(), tableName, columnNames, columnTypes, primaryIndex);

			thenTestForbiddenClasses(table, exempt);

			return table;
		}
		catch (AssertionError e) {
			throw e;
		}
		catch (Exception e) {
			fail("Unexpected exception in constructor", e);
		}
		return null;
	}

	void thenTestForbiddenClasses(Table table, List<String> exempt) {
		var forbidden = new LinkedHashSet<Class<?>>();

		Class<?> c = table.getClass();
		while (c != null)  {
			var fields = new HashSet<Field>();
			Collections.addAll(fields, c.getFields());
			Collections.addAll(fields, c.getDeclaredFields());

			for (Field f: fields) {
				try {
					f.setAccessible(true);

					var obj = f.get(table);
					if (obj != null) {
						var type = obj.getClass();

						while (type.isArray())
							type = type.getComponentType();

						if (type.isPrimitive() || type.isEnum())
							continue;

						if (exempt.contains(type.getTypeName()))
							continue;

						if (exempt.contains(type.getPackage().getName()))
							continue;

						if (type.getEnclosingClass() != null)
							if (exempt.contains(type.getEnclosingClass().getName()))
								continue;

						forbidden.add(type);
					}
				}
				catch (Exception e) {
					continue;
				}
				finally {
					f.setAccessible(false);
				}
			}

			c = c.getSuperclass();
		}

		if (forbidden.size() > 0)
			fail("Forbidden classes <%s> in table fields".formatted(forbidden));
	}

	DynamicTest testTableName() {
		return dynamicTest("tableName: %s".formatted(encode(tableName)), () -> {
			assertTimeout(ofMillis(TIMEOUT_MILLIS), () -> {
				assertEquals(
					tableName,
					subject.getTableName(),
					"%s has incorrect table name in schema".formatted(tableName)
				);
	        }, "Timeout in getTableName (infinite loop/recursion likely)");

			passed++;
		});
	}

	DynamicTest testColumnNames() {
		return dynamicTest("columnNames: %s".formatted(encode(columnNames)), () -> {
			assertTimeout(ofMillis(TIMEOUT_MILLIS), () -> {
				assertEquals(
					columnNames,
					subject.getColumnNames(),
					"%s has incorrect column names in schema".formatted(tableName)
				);
	        }, "Timeout in getColumnNames (infinite loop/recursion likely)");

			passed++;
		});
	}

	DynamicTest testColumnTypes() {
		return dynamicTest("columnTypes: %s".formatted(encode(columnTypes)), () -> {
			assertTimeout(ofMillis(TIMEOUT_MILLIS), () -> {
				assertEquals(
					columnTypes,
					subject.getColumnTypes(),
					"%s has incorrect column types in schema".formatted(tableName)
				);
	        }, "Timeout in getColumnTypes (infinite loop/recursion likely)");

			passed++;
		});
	}

	DynamicTest testPrimaryIndex() {
		return dynamicTest("primaryIndex: %s".formatted(encode(primaryIndex)), () -> {
			assertTimeout(ofMillis(TIMEOUT_MILLIS), () -> {
				assertEquals(
					primaryIndex,
					subject.getPrimaryIndex(),
					"%s has incorrect primary index in schema".formatted(tableName)
				);
	        }, "Timeout in getPrimaryIndex (infinite loop/recursion likely)");

			passed++;
		});
	}

	DynamicTest testClear() {
		var call = "clear()";
		logCall(tableName, call);

		return dynamicTest(call, () -> {
			control.clear();

			assertTimeoutPreemptively(ofMillis(TIMEOUT_MILLIS*10), () -> {
				subject.clear();
	        }, "Timeout in clear (infinite loop/recursion likely)");

			thenTestSize(call);
			thenTestFingerprint(call);

			passed++;
		});
	}

	DynamicTest testPut(boolean hitting, boolean primality) {
		var row = row(control.keyCache(), hitting);
		var key = row.get(primaryIndex);
		var call = "put(%s)".formatted(encode(row));
		logCall(tableName, call);

		return dynamicTest(title(call, key), () -> {
			var e_row = control.get(key);
			var hit = e_row != null;
			if (hit) hits++;
			else misses++;

			control.put(row);

			var result = assertTimeoutPreemptively(ofMillis(TIMEOUT_MILLIS), () -> {
		    	return subject.put(row);
		    }, "Timeout in put (infinite loop/recursion likely)");

			if (hit)
				assertTrue(result, "Expected %s to hit for key %s".formatted(call, key));
			else
				assertFalse(result, "Expected %s to miss for key %s".formatted(call, key));

			thenTestSize(call);
			thenTestFingerprint(call);
			if (primality) thenTestPrimality(call);

			passed++;
		});
	}

	DynamicTest testRemove(boolean hitting, boolean primality) {
		var key = key(control.keyCache(), hitting);
		var call = "remove(%s)".formatted(encode(key));
		logCall(tableName, call);

		return dynamicTest(title(call, key), () -> {
			var e_row = control.get(key);
			var hit = e_row != null;
			if (hit) hits++;
			else misses++;

			control.remove(key);

			var result = assertTimeoutPreemptively(ofMillis(TIMEOUT_MILLIS), () -> {
	        	return subject.remove(key);
	        }, "Timeout in remove (infinite loop/recursion likely)");

			if (hit)
				assertTrue(result, "Expected %s to hit for key %s".formatted(call, key));
			else
				assertFalse(result, "Expected %s to miss for key %s".formatted(call, key));

			thenTestSize(call);
			thenTestFingerprint(call);
			if (primality) thenTestPrimality(call);

			passed++;
		});
	}

	DynamicTest testGet(boolean hitting) {
		var key = key(control.keyCache(), hitting);
		var call = "get(%s)".formatted(encode(key));
		logCall(tableName, call);

		return dynamicTest(title(call, key), () -> {
			var e_row = control.get(key);
			var hit = e_row != null;
			if (hit) hits++;
			else misses++;

			var actual = assertTimeoutPreemptively(ofMillis(TIMEOUT_MILLIS), () -> {
	        	return subject.get(key);
	        }, "Timeout in get (infinite loop/recursion likely)");

			if (hit)
				assertEquals(
					e_row,
					actual,
					"Expected %s to hit for key %s and return the row".formatted(call, key)
				);
			else
				assertNull(actual, "Expected %s to miss for key %s and return null".formatted(key, call));

			thenTestSize(call);

			passed++;
		});
	}

	void thenTestSize(String after) {
		var expected = control.size();

		var actual = assertTimeout(ofMillis(TIMEOUT_MILLIS), () -> {
        	return subject.size();
        }, "After %s, timeout in size (infinite loop/recursion likely)".formatted(after));

		assertEquals(
			expected,
			actual,
			"After %s, table size is off by %d".formatted(after, actual - expected)
		);
	}

	void thenTestFingerprint(String after) {
		var result = assertTimeoutPreemptively(ofMillis(TIMEOUT_MILLIS), () -> {
        	return subject.hashCode();
        }, "After %s, timeout in fingerprint (infinite loop/recursion likely)".formatted(after));

		assertEquals(
			control.hashCode(),
			result,
			"After %s, fingerprint is off by %d".formatted(after, result - control.hashCode())
		);
	}

	void thenTestPrimality(String after) {
		var actual = assertTimeout(ofMillis(TIMEOUT_MILLIS), () -> {
        	return subject.capacity();
        }, "After %s, timeout in capacity (infinite loop/recursion likely)".formatted(after));

		assertEquals(
			"prime",
			actual % 2 != 0 && BigInteger.valueOf(actual).isProbablePrime(3) ? "prime" : (actual >= 2 ? "composite" : "non-prime"),
			"After %s, table capacity %d is not prime".formatted(after, actual)
		);
	}

	DynamicTest testIterator() {
		var call = "iterator traverses rows";

		return dynamicTest(title(call), () -> {
			var size = control.size();
			assertEquals(
				size,
				assertTimeoutPreemptively(ofMillis(TIMEOUT_MILLIS*10), () -> {
					var iter = subject.iterator();

					assertNotNull(iter, "Iterator must not be null");

					var rows = 0;
					while (true) {
						var has = false;
						try {
							has = iter.hasNext();
					    }
						catch (Exception e) {
							fail("Iterator's hasNext must not throw exceptions", e);
						}

						if (!has) break;

						Object row = null;
						try {
							row = iter.next();
					    }
						catch (Exception e) {
							fail("Iterator's next must not throw exceptions", e);
						}

						assertNotNull(
							row,
							"Iterator's next must not return null"
						);

						rows++;
					}
					return rows;
		        }, "Timeout in iterator (infinite loop/recursion likely)"),
				"Iterator must traverse the correct number of rows"
			);

			passed++;
		});
	}

	double hitRate() {
		return (double) hits / (hits + misses);
	}

//	@AfterAll
//	void auditHitRate() {
//		System.err.println(hitRate());
//	}

	String title(String call) {
		try {
			return assertTimeout(ofMillis(TIMEOUT_MILLIS), () -> {
				return "%s when \u03B1=%d/%d=%.3f".formatted(
					call,
					subject.size(),
					subject.capacity(),
					subject.loadFactor()
				);
	        });
		}
		catch (AssertionError e) {
			return "%s".formatted(call);
		}
	}

	String title(String call, Object key) {
		try {
			return assertTimeout(ofMillis(TIMEOUT_MILLIS), () -> {
				return "%s %s %s when \u03B1=%d/%d=%.3f".formatted(
					call,
					control.contains(key) ? "hits" : "misses",
					encode(key),
					subject.size(),
					subject.capacity(),
					subject.loadFactor()
				);
	        });
		}
		catch (AssertionError e) {
			return "%s %s %s".formatted(
				call,
				control.contains(key) ? "hits" : "misses",
				encode(key)
			);
		}
	}

	String encode(List<Object> row) {
		return encode(row, true);
	}

	String encode(List<?> row, boolean checkNulls) {
		var sb = new StringBuilder();
		if (checkNulls && row.contains(null))
			sb.append("Arrays.asList(");
		else
			sb.append("List.of(");
		for (var i = 0; i < row.size(); i++) {
			var field = row.get(i);
			if (i > 0)
				sb.append(", ");
			sb.append(encode(field));
		}
		sb.append(")");
		return sb.toString();
	}

	String encode(Object obj) {
		if (obj == null)
			return "null";
		else if (obj instanceof String)
			return "\"" + obj + "\"";
		else
			return obj.toString();
	}

	List<String> names(int width) {
		var names = new LinkedList<String>();
		while (names.size() < width) {
			var ns = ns();
			if (!names.contains(ns))
				names.add(ns);
		}
		return names;
	}

	List<FieldType> types(int width) {
		var types = new LinkedList<FieldType>();
		for (var i = 0; i < width; i++) {
			var type = type(i == primaryIndex);
			types.add(type);
		}
		return types;
	}

	FieldType type(boolean asPrimary) {
		return switch (RNG.nextInt(asPrimary ? 2 : 3)) {
			case 0 -> STRING;
			case 1 -> INTEGER;
			case 2 -> BOOLEAN;
			default -> null;
		};
	}

	List<Object> row(Set<Object> keyCache, boolean hitting) {
		var row = new LinkedList<>();
		for (var i = 0; i < columnTypes.size(); i++) {
			if (i != primaryIndex) {
				if (RNG.nextDouble() < 0.99)
					row.add(field(columnTypes.get(i)));
				else
					row.add(null);
			}
			else {
				row.add(key(keyCache, hitting));
			}
		}
		return row;
	}

	Object key(Set<Object> keyCache, boolean hitting) {
		var type = columnTypes.get(primaryIndex);
		if (hitting && keyCache.size() > 10) {
			var iter = keyCache.iterator();
			var key = iter.next();
			iter.remove();
			keyCache.add(key);
			return key;
		}
		else return field(type);
	}

	Object field(FieldType type) {
		return switch (type) {
			case STRING -> s();
			case INTEGER -> i();
			case BOOLEAN -> b();
			default -> null;
		};
	}

	static final String STRING_ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*+-";
	String s() {
		return s(STRING_ALPHABET, 0, 8);
	}
	String s(String alphabet, int lower, int upper) {
		var len = RNG.nextInt(upper);
		var sb = new StringBuilder();
		while (sb.length() < lower || sb.length() < len)
			sb.append(alphabet.charAt(RNG.nextInt(alphabet.length())));
		return sb.toString();
	}

	static final String NAME_ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
	String ns() {
		while (true) {
			var s = s(NAME_ALPHABET, 1, 16);
			if (Character.isLetter(s.charAt(0)))
				return s;
		}
	}

	int i() {
		return (int) (RNG.nextGaussian() * 1000);
	}

	Boolean b() {
		return RNG.nextBoolean();
	}

	void logStart(String suffix) {
		try {
			var path = LOGS_DIRECTORY.resolve(suffix == null
				? "%s.java".formatted(tableName)
				: "%s_%s.java".formatted(tableName, suffix)
			);

			Files.createDirectories(path.getParent());
			log = new PrintStream(path.toFile());
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	void logStart() {
		logStart(null);
	}

	void logLine(String line) {
		if (log != null)
			log.println(line);
	}

	void logConstructor(String className, String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex) {
		logLine("Table %s = new %s(%s, %s, %s, %s);".formatted(
			tableName,
			className,
			encode(tableName),
			encode(columnNames, false),
			encode(columnTypes, false),
			encode(primaryIndex)
		));
	}

	void logConstructor(String className, String tableName) {
		logLine("%s = new %s(%s);".formatted(
			tableName,
			className,
			encode(tableName)
		));
	}

	void logCall(String tableName, String call) {
		if (log != null)
			logLine("%s.%s;".formatted(tableName, call));
	}

	static class ControlTable extends Table {
		Map<Object, List<Object>> map;
		Set<Object> keyCache;
		int fingerprint;

		ControlTable(String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex) {
			setTableName(tableName);
			setColumnNames(columnNames);
			setColumnTypes(columnTypes);
			setPrimaryIndex(primaryIndex);

			map = new HashMap<>();
			keyCache = new LinkedHashSet<>();
			clear();
		}

		Set<Object> keyCache() {
			return keyCache;
		}

		@Override
		public void clear() {
			map.clear();
			keyCache.clear();
			fingerprint = 0;
		}

		@Override
		public boolean put(List<Object> row) {
			var key = row.get(getPrimaryIndex());
			var put = map.put(key, row);
			if (put != null) {
				keyCache.remove(key);
				keyCache.add(key);
				fingerprint += row.hashCode() - put.hashCode();
				return true;
			}
			keyCache.add(key);
			fingerprint += row.hashCode();
			return false;
		}

		@Override
		public boolean remove(Object key) {
			var rem = map.remove(key);
			if (rem != null) {
				keyCache.remove(key);
				fingerprint -= rem.hashCode();
				return true;
			}
			return false;
		}

		@Override
		public List<Object> get(Object key) {
			return map.get(key);
		}

		@Override
		public boolean contains(Object key) {
			return map.containsKey(key);
		}

		@Override
		public int size() {
			return map.size();
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
			return map.values().iterator();
		}
	}
}