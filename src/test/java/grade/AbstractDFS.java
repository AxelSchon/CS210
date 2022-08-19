package grade;

import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static sql.FieldType.*;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.api.function.ThrowingSupplier;

import sql.FieldType;
import tables.Table;

public abstract class AbstractDFS {
	/**
	 * A seed for the random number generator, or
	 * <code>null</code> to use a random seed.
	 * <p>
	 * The seed is used to generate a random sequence
	 * of table testing calls. To repeat a sequence,
	 * reuse the seed reported when testing it.
	 * <p>
	 * You may reassign this when debugging.
	 */
	public static final Integer RANDOM_SEED = null;

	/**
	 * The maximum time a table operation can
	 * execute in milliseconds before timeout.
	 * <p>
	 * You may reassign this when debugging.
	 */
	public static final int TIMEOUT_MILLIS = 100;

	/**
	 * The percentage of puts, gets, and removes
	 * which should be hits, approximately.
	 * <p>
	 * You may reassign this when debugging.
	 */
	public static final double TARGET_HIT_RATE = .60;

	protected static int ungraded, passed, tested_tables;

	protected static String module_tag;
	protected static int calls_per_table;

	protected static Table actualTable;
	protected static Table expectedTable;

	protected static Random RNG;
	protected static int actual_seed;
	protected static PrintStream LOG_FILE;

	@BeforeAll
	protected static final void initialize() throws IOException {
		ungraded = 0;
		passed = 0;

		actualTable = null;
		expectedTable = null;

		if (RANDOM_SEED != null) {
			actual_seed = RANDOM_SEED;
			RNG = new Random(actual_seed);
		}
		else {
			RNG = new Random();
			actual_seed = Math.abs(RNG.nextInt());
			RNG.setSeed(actual_seed);
		}
		System.out.printf("Random Seed: %d\n\n", actual_seed);
	}

	public abstract Stream<DynamicTest> testTable(String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex);

	protected static final Table firstTestConstructor(ThrowingSupplier<Table> supplier) {
		Table table = null;
		try {
			table = assertTimeout(ofMillis(TIMEOUT_MILLIS*100),
				supplier,
				"Timeout in constructor (infinite loop/recursion likely)"
			);
		}
		catch (AssertionError e) {
			throw e;
		}
		catch (Exception e) {
			fail("Unexpected exception in constructor", e);
		}
		return table;
	}

	protected static final DynamicTest testTableName(String tableName) {
		final var call = "getTableName() yields %s".formatted(tableName);
		logCall(tableName, call);

		return dynamicTest(call, () -> {
			assertTimeout(ofMillis(TIMEOUT_MILLIS), () -> {
				assertEquals(
					tableName,
					actualTable.getTableName(),
					"%s has incorrect table name in schema".formatted(tableName)
				);
	        }, "Timeout in getTableName (infinite loop/recursion likely)");

			passed++;
		});
	}

	protected static final DynamicTest testColumnNames(String tableName, List<String> columnNames) {
		final var call = "getColumnNames() yields %s".formatted(columnNames);
		logCall(tableName, call);

		return dynamicTest(call, () -> {
			assertTimeout(ofMillis(TIMEOUT_MILLIS), () -> {
				assertEquals(
					columnNames,
					actualTable.getColumnNames(),
					"%s has incorrect column names in schema".formatted(tableName)
				);
	        }, "Timeout in getColumnNames (infinite loop/recursion likely)");

			passed++;
		});
	}

	protected static final DynamicTest testColumnTypes(String tableName, List<FieldType> columnTypes) {
		final var call = "getColumnTypes() yields %s".formatted(columnTypes);
		logCall(tableName, call);

		return dynamicTest(call, () -> {
			assertTimeout(ofMillis(TIMEOUT_MILLIS), () -> {
				assertEquals(
					columnTypes,
					actualTable.getColumnTypes(),
					"%s has incorrect column types in schema".formatted(tableName)
				);
	        }, "Timeout in getColumnTypes (infinite loop/recursion likely)");

			passed++;
		});
	}

	protected static final DynamicTest testPrimaryIndex(String tableName, int primaryIndex) {
		final var call = "getPrimaryIndex() yields %s".formatted(primaryIndex);
		logCall(tableName, call);

		return dynamicTest(call, () -> {
			assertTimeout(ofMillis(TIMEOUT_MILLIS), () -> {
				assertEquals(
					primaryIndex,
					actualTable.getPrimaryIndex(),
					"%s has incorrect primary index in schema".formatted(tableName)
				);
	        }, "Timeout in getPrimaryIndex (infinite loop/recursion likely)");

			passed++;
		});
	}

	protected static final DynamicTest testClear(String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex) {
		final var call = "clear()";
		logCall(tableName, call);

		return dynamicTest(call, () -> {
			expectedTable.clear();

			assertTimeoutPreemptively(ofMillis(TIMEOUT_MILLIS*10), () -> {
				actualTable.clear();
	        }, "Timeout in clear (infinite loop/recursion likely)");

			thenTestSize(call);
			thenTestFingerprint(call);

			passed++;
		});
	}

	protected static final DynamicTest testPut(String tableName, List<FieldType> columnTypes, int primaryIndex) {
		final var row = row(columnTypes, primaryIndex);
		final var key = row.get(primaryIndex);
		final var call = "put(%s)".formatted(encode(row));
		logCall(tableName, call);

		return dynamicTest(title(call, key), () -> {
			var e_row = expectedTable.get(key);
			var hit = e_row != null;
			if (hit) put_hits++;
			puts++;

			expectedTable.put(row);

			var result = assertTimeoutPreemptively(ofMillis(TIMEOUT_MILLIS), () -> {
	        	return actualTable.put(row);
	        }, "Timeout in put (infinite loop/recursion likely)");

			if (hit)
				assertTrue(result, "Expected %s to hit for key %s".formatted(call, key));
			else
				assertFalse(result, "Expected %s to miss for key %s".formatted(call, key));

			thenTestSize(call);
			thenTestFingerprint(call);

			passed++;
		});
	}

	protected static final DynamicTest testRemove(String tableName, List<FieldType> columnTypes, int primaryIndex) {
		final var key = f(columnTypes.get(primaryIndex), true);
		final var call = "remove(%s)".formatted(encode(key));
		logCall(tableName, call);

		return dynamicTest(title(call, key), () -> {
			var e_row = expectedTable.get(key);
			var hit = e_row != null;
			if (hit) rem_hits++;
			rems++;

			expectedTable.remove(key);

			var result = assertTimeoutPreemptively(ofMillis(TIMEOUT_MILLIS), () -> {
	        	return actualTable.remove(key);
	        }, "Timeout in remove (infinite loop/recursion likely)");

			if (hit)
				assertTrue(result, "Expected %s to hit for key %s".formatted(call, key));
			else
				assertFalse(result, "Expected %s to miss for key %s".formatted(call, key));

			thenTestSize(call);
			thenTestFingerprint(call);

			passed++;
		});
	}

	protected static final DynamicTest testGet(String tableName, List<FieldType> columnTypes, int primaryIndex) {
		final var key = f(columnTypes.get(primaryIndex), true);
		final var call = "get(%s)".formatted(encode(key));
		logCall(tableName, call);

		return dynamicTest(title(call, key), () -> {
			var e_row = expectedTable.get(key);
			var hit = e_row != null;
			if (hit) get_hits++;
			gets++;

			var actual = assertTimeoutPreemptively(ofMillis(TIMEOUT_MILLIS), () -> {
	        	return actualTable.get(key);
	        }, "Timeout in get (infinite loop/recursion likely)");

			if (hit)
				assertEquals(
					e_row,
					actual,
					"Expected %s to hit for key %s and return row <%s>".formatted(call, key, e_row)
				);
			else
				assertNull(actual, "Expected %s to miss for key %s and return null".formatted(key, call));

			thenTestSize(call);

			passed++;
		});
	}

	protected static final void thenTestSize(String after) {
		var expected = expectedTable.size();

		var actual = assertTimeout(ofMillis(TIMEOUT_MILLIS), () -> {
        	return actualTable.size();
        }, "After %s, timeout in size (infinite loop/recursion likely)".formatted(after));

		assertEquals(
			expected,
			actual,
			"After %s, table size is off by %d".formatted(after, actual - expected)
		);
	}

	protected static final void thenTestFingerprint(String after) {
		var result = assertTimeoutPreemptively(ofMillis(TIMEOUT_MILLIS*10), () -> {
        	return actualTable.hashCode();
        }, "After %s, timeout in fingerprint (infinite loop/recursion likely)".formatted(after));

		assertEquals(
			expectedTable.hashCode(),
			result,
			"After %s, fingerprint is off by %d".formatted(after, result - expectedTable.hashCode())
		);
	}

	protected static final DynamicTest testIterator() {
		final var call = "iterator traverses rows";

		return dynamicTest(title(call), () -> {
			var size = expectedTable.size();
			assertEquals(
				size,
				assertTimeoutPreemptively(ofMillis(TIMEOUT_MILLIS*10), () -> {
					var iter = actualTable.iterator();

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

	protected static final void testForbiddenClasses(Object subject, Class<?> cls, List<String> exempt) {
		if (actualTable == null)
			fail("Depends on constructor prerequisite");

		final var forbidden = new HashSet<Class<?>>();

		for (Class<?> clazz = cls; clazz != null; clazz = clazz.getSuperclass()) {
			final var fields = new HashSet<Field>();
			Collections.addAll(fields, clazz.getFields());
			Collections.addAll(fields, clazz.getDeclaredFields());

			for (Field f: fields) {
				try {
					f.setAccessible(true);

					var obj = f.get(subject);
					if (obj != null) {
						var type = obj.getClass();

						while (type.isArray())
							type = type.getComponentType();

						if (type.isPrimitive() )
							continue;

						if (exempt.contains(type.getTypeName()))
							continue;

						if (exempt.contains(type.getPackage().getName()))
							continue;

						if (type.getEnclosingClass() != null)
							if(exempt.contains(type.getEnclosingClass().getName()))
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
		}

		if (forbidden.size() > 0) {
			System.err.println("Unexpected forbidden classes:");
			forbidden.forEach(System.err::println);
			System.err.println();

			actualTable = null;
			fail("Unexpected forbidden classes <%s>".formatted(forbidden));
		}
	}

	private static int puts, put_hits;
	private static int rems, rem_hits;
	private static int gets, get_hits;

	@AfterAll
	public static final void report(TestReporter reporter) {
		var graded = calls_per_table * tested_tables;
		var earned = (int) Math.ceil(passed / (double) graded * 100);

		System.out.println();

		var put_hit_rate = ((double) put_hits / puts) * 100;
		System.out.printf(
			"Puts: %,d (%.0f%% Hit, %.0f%% Miss)\n",
			puts,
			put_hit_rate,
			100 - put_hit_rate
		);

		var rem_hit_rate = ((double) rem_hits / rems) * 100;
		System.out.printf(
			"Rems: %,d (%.0f%% Hit, %.0f%% Miss)\n",
			rems,
			rem_hit_rate,
			100 - rem_hit_rate
		);

		var get_hit_rate = ((double) get_hits / gets) * 100;
		System.out.printf(
			"Gets: %,d (%.0f%% Hit, %.0f%% Miss)\n",
			gets,
			get_hit_rate,
			100 - get_hit_rate
		);

		System.out.printf(
			"Misc: %,d\n",
			graded - puts - gets - rems
		);

		System.out.println();

		if (ungraded > 0)
			System.out.println("Prerequisites: %s".formatted(ungraded));
		System.out.println("Passed Tests: %,d".formatted(passed));
		System.out.println("Graded Tests: %,d".formatted(graded));
		System.out.println("Module Grade: %,d%%".formatted(earned));

		System.out.println();

		System.out.printf(
			"[%s PASSED %d%% OF UNIT TESTS]\n",
			module_tag,
			earned
		);

		System.out.println();

		reporter.publishEntry(module_tag, String.valueOf(earned));
	}

	protected static final String s(String alphabet, int lower, int upper) {
		var len = RNG.nextInt(upper);
		var sb = new StringBuilder();
		while (sb.length() < lower || sb.length() < len)
			sb.append(alphabet.charAt(RNG.nextInt(alphabet.length())));
		return sb.toString();
	}

	protected static final String s() {
		return s(ALPHA_STRING, 0, 8);
	}

	private static final String ALPHA_STRING = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*+-";
	private static Queue<String> ks_queue = new LinkedList<>();
	protected static final String ks() {
		if (ks_queue.size() < 10 || rate() > TARGET_HIT_RATE) {
			while (true) {
				var s = s();
				if (!ks_queue.contains(s)) {
					ks_queue.offer(s);
					return s;
				}
			}
		}
		else {
			var s = ks_queue.poll();
			ks_queue.offer(s);
			return s;
		}
	}

	protected final List<String> lns(int width) {
		var names = new LinkedList<String>();
		for (var i = 0; i < width; i++)
			names.add(ns());
		return names;
	}

	private static final String ALPHA_NAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
	private static Set<String> ns_cache = new HashSet<>();
	protected static final String ns() {
		while (true) {
			var s = s(ALPHA_NAME, 1, 16);
			if (Character.isLetter(s.charAt(0)) && ns_cache.add(s))
				return s;
		}
	}

	protected static final int i() {
		return (int) (RNG.nextGaussian() * 1000);
	}

	private static Queue<Integer> ki_queue = new LinkedList<>();
	protected static final Integer ki() {
		if (ki_queue.size() < 10 || rate() > TARGET_HIT_RATE) {
			while (true) {
				var i = i();
				if (!ki_queue.contains(i)) {
					ki_queue.offer(i);
					return i;
				}
			}
		}
		else {
			var i = ki_queue.poll();
			ki_queue.offer(i);
			return i;
		}
	}

	protected static final Boolean b() {
		return RNG.nextBoolean();
	}

	protected static final FieldType t(boolean primary) {
		return switch (RNG.nextInt(3)) {
			case 0 -> STRING;
			case 1 -> INTEGER;
			case 2 -> !primary ? BOOLEAN : t(primary);
			default -> null;
		};
	}

	private static String tableNameCache;
	private static List<String> columnNamesCache;
	private static List<FieldType> columnTypesCache;
	private static int primaryIndexCache;

	protected final Stream<DynamicTest> makeTable(String tableName, int lower, int upper) {
		var width = RNG.nextInt(upper-lower) + lower;
		tableNameCache = tableName;
		primaryIndexCache = RNG.nextInt(width);
		columnNamesCache = new LinkedList<>();
		columnTypesCache = new LinkedList<>();
		for (var i = 0; i < width; i++) {
			columnNamesCache.add(ns());
			columnTypesCache.add(t(i == primaryIndexCache));
		}
		return testTable(tableNameCache, columnNamesCache, columnTypesCache, primaryIndexCache);
	}

	protected final Stream<DynamicTest> reuseTable() {
		return testTable(tableNameCache, columnNamesCache, columnTypesCache, primaryIndexCache);
	}

	protected static final Object f(FieldType type, boolean key) {
		return switch (type) {
			case STRING -> key ? ks() : s();
			case INTEGER -> key ? ki() : i();
			case BOOLEAN -> b();
			default -> null;
		};
	}

	protected static final List<Object> row(List<FieldType> columnTypes, int primaryIndex) {
		final var row = new LinkedList<>();
		for (var i = 0; i < columnTypes.size(); i++) {
			if (i != primaryIndex) {
				if (RNG.nextDouble() < 0.99)
					row.add(f(columnTypes.get(i), false));
				else
					row.add(null);
			}
			else {
				row.add(f(columnTypes.get(i), true));
			}
		}
		return row;
	}

	protected static final double rate() {
		return (double) (put_hits + get_hits + rem_hits) / (puts + gets+ rems);
	}

	protected static final String encode(List<Object> row) {
		return encode(row, true);
	}

	protected static final String encode(List<?> row, boolean checkNulls) {
		final var sb = new StringBuilder();
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

	protected static final String encode(Object obj) {
		if (obj == null)
			return "null";
		else if (obj instanceof String)
			return "\"" + obj + "\"";
		else
			return obj.toString();
	}

	protected static final String title(String call) {
		try {
			return assertTimeout(ofMillis(TIMEOUT_MILLIS), () -> {
				return "%s when \u03B1=%d/%d=%.3f".formatted(
					call,
					actualTable.size(),
					actualTable.capacity(),
					actualTable.loadFactor()
				);
	        });
		}
		catch (AssertionError e) {
			return "%s".formatted(
				call
			);
		}
	}

	protected static final String title(String call, Object key) {
		try {
			return assertTimeout(ofMillis(TIMEOUT_MILLIS), () -> {
				return "%s %s %s when \u03B1=%d/%d=%.3f".formatted(
					call,
					expectedTable.contains(key) ? "hits" : "misses",
					encode(key),
					actualTable.size(),
					actualTable.capacity(),
					actualTable.loadFactor()
				);
	        });
		}
		catch (AssertionError e) {
			return "%s %s %s".formatted(
				call,
				expectedTable.contains(key) ? "hits" : "misses",
				encode(key)
			);
		}
	}

	protected static final void startLog(String tableName) {
		try {
			var path = Paths.get("data", "logs", "%s.java".formatted(tableName));

			System.out.println("Log: %s".formatted(path));

			Files.createDirectories(path.getParent());
			LOG_FILE = new PrintStream(path.toFile());
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected static final void logRandomSeed() {
		logLine("// Random Seed: %d".formatted(actual_seed));
	}

	protected static final void logConstructor(String className, String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex) {
		logLine("Table %s = new %s(%s, %s, %s, %s);".formatted(
			tableName,
			className,
			encode(tableName),
			encode(columnNames, false),
			encode(columnTypes, false),
			encode(primaryIndex)
		));
	}

	protected static final void logConstructor(String className, String tableName) {
		logLine("%s = new %s(%s);".formatted(
			tableName,
			className,
			encode(tableName)
		));
	}

	protected static final void logLine(String line) {
		if (LOG_FILE != null)
			LOG_FILE.println(line);
	}

	protected static final void logCall(String tableName, String call) {
		if (LOG_FILE != null)
			LOG_FILE.printf("%s.%s;\n", tableName, call);
	}

	protected static class ProxyTable extends Table {
		private Map<Object, List<Object>> map;
		private int fingerprint;

		public ProxyTable(String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex) {
			setTableName(tableName);
			setColumnNames(columnNames);
			setColumnTypes(columnTypes);
			setPrimaryIndex(primaryIndex);

			map = new HashMap<>();
			clear();
		}

		@Override
		public void clear() {
			map.clear();
			fingerprint = 0;
		}

		@Override
		public boolean put(List<Object> row) {
			var key = row.get(getPrimaryIndex());
			var put = map.put(key, row);
			if (put != null) {
				fingerprint += row.hashCode() - put.hashCode();
				return true;
			}
			fingerprint += row.hashCode();
			return false;
		}

		@Override
		public boolean remove(Object key) {
			var rem = map.remove(key);
			if (rem != null) {
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
			return new Iterator<>() {
				Iterator<List<Object>> iter = map.values().iterator();

				@Override
				public boolean hasNext() {
					return iter.hasNext();
				}

				@Override
				public List<Object> next() {
					return iter.next();
				}
			};
		}
	}
}