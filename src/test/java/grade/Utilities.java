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
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import apps.Database;
import sql.FieldType;
import sql.QueryError;
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

			System.out.println("Log: " + path);
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
}

abstract class AbstractQueryContainer {
	static final Path LOGS_DIRECTORY = Paths.get("data", "logs");

	int graded, earned;

	Object[][] queryData;
	Object[][] controlData;

	Database subject;
	PrintStream log;

	@BeforeAll
	void startGrade() {
		graded = 0;
		earned = 0;
	}

	@BeforeAll
	void defineDB() throws IOException {
		try {
			subject = new Database(false);
		}
		catch (Exception e) {
			fail("Database constructor must not throw exceptions", e);
		}
	}

	@BeforeAll
	void defineLog() {
		logStart();
	}

	Arguments[] injectParams() {
		var arguments = new Arguments[queryData.length];

		for (var a = 0; a < arguments.length; a++) {
			Table table = null;
			if (a < controlData.length && controlData[a] != null) {
				var i = 0;

				var tableName = (String) controlData[a][i++];
				var columnCount = (int) controlData[a][i++];
				var primaryIndex = (int) controlData[a][i++];

				var columnNames = new LinkedList<String>();
				for (var j = 1; j <= columnCount; j++)
					columnNames.add((String) controlData[a][i++]);

				var columnTypes = new LinkedList<FieldType>();
				for (var j = 1; j <= columnCount; j++)
					columnTypes.add((FieldType) controlData[a][i++]);

				List<List<Object>> rows = new LinkedList<>();
				for (var j = i; j < controlData[a].length; j += columnCount) {
					var row = new LinkedList<>();
					for (var k = 0; k < columnCount; k++)
						row.add(controlData[a][j+k]);
					rows.add(row);
				}

				table = new ControlTable(
					tableName,
					columnNames,
					columnTypes,
					primaryIndex,
					rows
				);
			}

			arguments[a] = Arguments.of(
				queryData[a][1],
				Objects.toString(queryData[a][2], "none provided"),
				queryData[a][0],
				table
			);
		}

		return arguments;
	}

	@DisplayName("Queries")
	@ParameterizedTest(name = "[{index}] {0}")
	@MethodSource("injectParams")
	void testQuery(String query, String purpose, Object expectedResult, Table expectedTable) {
		if (!purpose.contains("prerequisite"))
			graded++;

		Object actualResult = null;
		try {
			logQuery(query);
			actualResult = subject.interpret(query);
		}
		catch (QueryError error) {
			actualResult = error;
		}
		catch (Exception thrown) {
			fail(
				"Query must not throw <%s> with reason: <%s>, purpose: <%s>".formatted(
					thrown.getClass(),
					Objects.toString(thrown.getMessage(), "none provided"),
					purpose
				),
				thrown
			);
		}

		Table actualTable = null;
		if (expectedResult == Table.class) {
			if (!(actualResult instanceof Table))
				assertEquals(
					Table.class,
					actualResult,
					"Query must return %s, purpose: <%s>".formatted(
						expectedTable != null && expectedTable.getTableName().startsWith("_") ? "result set" : "table",
						purpose
					)
				);

			actualTable = (Table) actualResult;
		}
		else if (expectedResult instanceof Integer) {
			assertEquals(
				expectedResult,
				actualResult,
				"Query must return integer (number of affected rows), purpose: <%s>".formatted(purpose)
			);

			var embeddedName = query.strip().split("\\s+")[2];
			for (var table: subject.tables()) {
				if (table.getTableName().equals(embeddedName)) {
					actualTable = table;
					break;
				}
			}
		}
		else if (expectedResult instanceof String) {
			assertEquals(
				expectedResult,
				actualResult,
				"Query must return string, purpose: <%s>".formatted(purpose)
			);
		}
		else if (expectedResult instanceof Boolean) {
			assertEquals(
				expectedResult,
				actualResult,
				"Query must return boolean, purpose: <%s>".formatted(purpose)
			);
		}
		else if (expectedResult == QueryError.class) {
			if (!(actualResult instanceof QueryError))
				assertEquals(
					QueryError.class,
					actualResult,
					"Query must throw SQLError, purpose: <%s>".formatted(purpose)
				);
		}

		if (expectedTable != null) {
			var friendlyName = friendly(expectedTable.getTableName());

			assertEquals(
				expectedTable.getTableName(),
				actualTable.getTableName(),
				"%s has incorrect table name in schema".formatted(friendlyName)
			);

			assertEquals(
				expectedTable.getColumnNames(),
				actualTable.getColumnNames(),
				"%s has incorrect column names in schema".formatted(friendlyName)
			);

			assertEquals(
				expectedTable.getColumnTypes(),
				actualTable.getColumnTypes(),
				"%s has incorrect column types in schema".formatted(friendlyName)
			);

			assertEquals(
				expectedTable.getPrimaryIndex(),
				actualTable.getPrimaryIndex(),
				"%s has incorrect primary index in schema".formatted(friendlyName)
			);

			for (var e_row: expectedTable) {
				var e_key = e_row.get(expectedTable.getPrimaryIndex());

				if (!actualTable.contains(e_key))
					fail(
						"%s doesn't contain expected key <%s> with type <%s> in state".formatted(
							friendlyName,
							e_key,
							typeOf(e_key)
						)
					);

				var a_row = actualTable.get(e_key);

				assertEquals(
					typesOf(e_row),
					typesOf(a_row),
					"%s has unexpected types of row <%s> in state".formatted(
						friendlyName,
						a_row
					)
				);

				assertEquals(
					stringsOf(e_row),
					stringsOf(a_row),
					"%s has unexpected field values of row with key <%s> in state".formatted(
						friendlyName,
						e_key
					)
				);
			}

			for (var a_key: actualTable.keys()) {
				if (!expectedTable.contains(a_key))
					fail(
						"%s contains unexpected key <%s> with type <%s> in state".formatted(
							friendlyName,
							a_key,
							typeOf(a_key)
						)
					);
			}
		}

		if (!purpose.contains("prerequisite"))
			earned++;

		serialTable = actualTable;
	}

	Table serialTable;

	@AfterAll
	void reportGrade(TestReporter reporter) throws IOException {
		var module = this.getClass().getSimpleName();
		var tag = "%s%s".formatted(module.charAt(0), module.charAt(module.length() - 1));
		var pct = (int) Math.ceil(earned / (double) graded * 100);

		System.out.printf("[%s PASSED %d%% OF UNIT TESTS]\n", tag, pct);

		reporter.publishEntry("Tag", tag);
		reporter.publishEntry("Grade", String.valueOf(pct));
	}

	@AfterAll
	void cleanDB() {
		try {
			subject.close();
		}
		catch (Exception e) {
			fail("Database close should not throw exceptions", e);
		}
	}

	static String friendly(String tableName) {
		return tableName.startsWith("_")
			? "result set <%s>".formatted(tableName)
			: "table <%s> in the database".formatted(tableName);
	}

	static String typeOf(Object obj) {
		if (obj == null)
			return "null";
		else if (obj instanceof String s)
			return "%s (length %d)".formatted(FieldType.STRING.toString(), s.length());
		else if (obj instanceof Integer i)
			return FieldType.INTEGER.toString();
		else if (obj instanceof Boolean b)
			return FieldType.BOOLEAN.toString();
		else
			return "%s (illegal)".formatted(obj.getClass().getSimpleName().toUpperCase());
	}

	static List<String> typesOf(List<Object> list) {
		if (list == null)
			return null;

		return list.stream().map(v -> typeOf(v)).collect(Collectors.toList());
	}

	static List<String> stringsOf(List<Object> list) {
		if (list == null)
			return null;

		return list.stream().map(v -> String.valueOf(v)).collect(Collectors.toList());
	}

	void logStart() {
		try {
			var module = this.getClass().getSimpleName();
			var tag = "%s%s".formatted(module.charAt(0), module.charAt(module.length() - 1));
			var path = LOGS_DIRECTORY.resolve("%s.sql".formatted(tag));

			Files.createDirectories(path.getParent());
			log = new PrintStream(path.toFile());

			System.out.println("Log: " + path);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	void logQuery(String query) {
		if (log != null)
			log.println(query + ";");
	}
}

class ControlTable extends Table {
	Map<Object, List<Object>> map;
	Set<Object> keyCache;
	int fingerprint;

	ControlTable(String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex) {
		setTableName(tableName);
		setColumnNames(columnNames);
		setColumnTypes(columnTypes);
		setPrimaryIndex(primaryIndex);

		keyCache = new LinkedHashSet<>();
		map = new HashMap<>();
	}

	public ControlTable(String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex, List<List<Object>> rows) {
		this(tableName, columnNames, columnTypes, primaryIndex);

		for (var row: rows) {
			var key = row.get(primaryIndex);
			map.put(key, row);
		}
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