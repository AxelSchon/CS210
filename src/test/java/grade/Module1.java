package grade;

import static sql.FieldType.*;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import tables.HashArrayTable;

@TestInstance(Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
final class Module1 {
	static final int CALLS_PER_TABLE = 2000;
	static final double TARGET_HIT_RATE = .60;

	int graded, earned;

	@BeforeAll
	void startGrade() {
		graded = 0;
		earned = 0;
	}

	@Nested
	@DisplayName("m1_table1 [3 known columns]")
	class TableContainer1 extends HashArrayTableContainer {
		@BeforeAll
		void defineTable() {
			tableName = "m1_table1";
			columnNames = List.of("ps", "i", "b");
			columnTypes = List.of(STRING, INTEGER, BOOLEAN);
			primaryIndex = 0;
		}
	}

	@Nested
	@DisplayName("m1_table2 [1 to 5 random columns]")
	class TableContainer2 extends HashArrayTableContainer {
		@BeforeAll
		void defineTable() {
			tableName = "m1_table2";
			var width = RNG.nextInt(1, 5+1);
			primaryIndex = RNG.nextInt(width);
			columnNames = names(width);
			columnTypes = types(width);
		}
	}

	@Nested
	@DisplayName("m1_table3 [5 to 15 random columns]")
	class TableContainer3 extends HashArrayTableContainer {
		@BeforeAll
		void defineTable() {
			tableName = "m1_table3";
			var width = RNG.nextInt(5, 15+1);
			primaryIndex = RNG.nextInt(width);
			columnNames = names(width);
			columnTypes = types(width);
		}
	}

	abstract class HashArrayTableContainer extends AbstractTableContainer {
		@DisplayName("New Table")
		@TestFactory
		@Execution(ExecutionMode.SAME_THREAD)
		Stream<DynamicTest> testNewTable() {
			logStart();

			subject = testConstructor(() -> {
				return new HashArrayTable(tableName, columnNames, columnTypes, primaryIndex);
	        }, List.of(
    			"tables",
    			"java.lang",
    			"java.util.ImmutableCollections"
    		));

			control = new ControlTable(tableName, columnNames, columnTypes, primaryIndex);

			return IntStream.range(0, CALLS_PER_TABLE).mapToObj(i -> {
				if (i == 0)
					return testTableName();
				else if (i == 1)
					return testColumnNames();
				else if (i == 2)
					return testColumnTypes();
				else if (i == 3)
					return testPrimaryIndex();
				else if (i == 4 || i == CALLS_PER_TABLE-1)
					return testClear();
				else if (i % 20 == 0 || i == CALLS_PER_TABLE-2)
					return testIterator();
				else {
					var p = RNG.nextDouble();
					var hitting = hitRate() < TARGET_HIT_RATE;
					if (p < 0.85)
						return testPut(hitting, true);
					else if (p < 0.95)
						return testRemove(hitting, true);
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