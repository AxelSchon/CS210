package grade;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import tables.HashFileTable;

@TestInstance(Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
final class Module2 {
	static final int CALLS_PER_TABLE = 1500;
	static final double TARGET_HIT_RATE = .60;

	int graded, earned;

	@BeforeAll
	void startGrade() {
		graded = 0;
		earned = 0;
	}

	@Nested
	@DisplayName("m2_table1 [5 to 15 random columns]")
	class TableContainer1 extends HashFileTableContainer {
		@BeforeAll
		void defineTable() {
			tableName = "m2_table1";
			var width = RNG.nextInt(1, 15+1);
			primaryIndex = RNG.nextInt(width);
			columnNames = names(width);
			columnTypes = types(width);
		}
	}

	@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
	abstract class HashFileTableContainer extends AbstractTableContainer {
		static final List<String> exempt = List.of(
			"tables",
			"java.lang",
			"java.util.ImmutableCollections",
			"java.nio",
			"sun.nio.ch",
			"sun.nio.cs",
			"sun.nio.fs"
		);

		@Order(1)
		@DisplayName("New Table")
		@TestFactory
		@Execution(ExecutionMode.SAME_THREAD)
		Stream<DynamicTest> testNewTable() {
			logStart("new");

			subject = testConstructor(() -> {
				return new HashFileTable(tableName, columnNames, columnTypes, primaryIndex);
	        }, exempt);

			control = new ControlTable(tableName, columnNames, columnTypes, primaryIndex);

			return IntStream.range(0, CALLS_PER_TABLE/2).mapToObj(i -> {
				if (i == 0)
					return testTableName();
				else if (i == 1)
					return testColumnNames();
				else if (i == 2)
					return testColumnTypes();
				else if (i == 3)
					return testPrimaryIndex();
				else if (i == 4)
					return testClear();
				else if (i % 20 == 0 || i == CALLS_PER_TABLE/2-1)
					return testIterator();
				else {
					var p = RNG.nextDouble();
					var hitting = hitRate() < TARGET_HIT_RATE;
					if (p < 0.95)
						return testPut(hitting, false);
					else
						return testRemove(hitting, false);
				}
			});
		}

		@Order(2)
		@DisplayName("Existing Table")
		@TestFactory
		@Execution(ExecutionMode.SAME_THREAD)
		Stream<DynamicTest> testExistingTable() {
			logStart("existing");

			subject = testConstructor(() -> {
				return new HashFileTable(tableName);
	        }, exempt);

			return IntStream.range(0, CALLS_PER_TABLE/2).mapToObj(i -> {
				if (i == 0)
					return testTableName();
				else if (i == 1)
					return testColumnNames();
				else if (i == 2)
					return testColumnTypes();
				else if (i == 3)
					return testPrimaryIndex();
				else if (i == 4 || i == CALLS_PER_TABLE/2-1)
					return testIterator();
				else {
					var p = RNG.nextDouble();
					var hitting = hitRate() < TARGET_HIT_RATE;
					if (p < 0.95)
						return testPut(hitting, false);
					else
						return testGet(hitting);
				}
			});
		}

		@AfterAll
		@ResourceLock(value = "graded")
		@ResourceLock(value = "earned")
		void accrueGrade() {
			graded += CALLS_PER_TABLE;
			earned += passed;
		}
	}

	@AfterAll
	void reportGrade(TestReporter reporter) {
		var module = this.getClass().getSimpleName();
		var tag = "%s%s".formatted(module.charAt(0), module.charAt(module.length() - 1));
		var pct = (int) Math.ceil(earned / (double) graded * 100);

		System.out.printf("[%s PASSED %d%% OF UNIT TESTS]\n", tag, pct);

		reporter.publishEntry("Tag", tag);
		reporter.publishEntry("Grade", String.valueOf(pct));
	}
}