package grade;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static sql.FieldType.*;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestMethodOrder;

import sql.FieldType;
import tables.SearchTable;

@Deprecated
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Practice1 extends AbstractDFS {
	@BeforeAll
	public static void setup() {
		module_tag = "P1";
		calls_per_table = 2000;
	}

	@TestFactory
    @DisplayName("Prerequisites")
    @Order(0)
    public final Stream<DynamicTest> audits() {
		return Stream.of(
			dynamicTest("Constructor (4-ary)", () -> {
				ungraded++;
				try {
					actualTable = firstTestConstructor(() -> {
						return new SearchTable(
							"p1_table00",
							List.of("a", "b", "c"),
							List.of(STRING, INTEGER, BOOLEAN),
							0
						);
			        });
				}
				catch (Exception e) {
					fail("Unexpected exception with 4-ary constructor", e);
				}
    		}),
			dynamicTest("Forbidden Classes", () -> {
				ungraded++;
				if (actualTable == null)
					fail("Depends on constructor prerequisite");

				testForbiddenClasses(
					actualTable,
					SearchTable.class,
					List.of(
						"tables",
						"java.lang",
						"java.util.ImmutableCollections",
						"java.util.TreeMap"
					)
				);
    		})
    	);
    }

	@TestFactory
	@DisplayName("Create m1_table01 [example columns]")
	@Order(1)
	public final Stream<DynamicTest> createTable01() {
		tested_tables++;
		return testTable(
			"m1_table01",
			List.of("ps", "i", "b"),
			List.of(STRING, INTEGER, BOOLEAN),
			0
		);
	}

	@TestFactory
	@DisplayName("Create m1_table02 [1 to 5 random columns]")
	@Order(1)
	public final Stream<DynamicTest> createTable02() {
		tested_tables++;
		return makeTable("m1_table02", 1, 5);
	}

	@TestFactory
	@DisplayName("Create m1_table03 [5 to 15 random columns]")
	@Order(1)
	public final Stream<DynamicTest> createTable03() {
		tested_tables++;
		return makeTable("m1_table03", 5, 15);
	}

	@Override
	public final Stream<DynamicTest> testTable(String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex) {
		startLog(tableName);

		actualTable = firstTestConstructor(() -> {
			return new SearchTable(
				tableName,
				columnNames,
				columnTypes,
				primaryIndex
			);
        });

		logRandomSeed();
		logConstructor("SearchTable", tableName, columnNames, columnTypes, primaryIndex);

		expectedTable = new ProxyTable(tableName, columnNames, columnTypes, primaryIndex);

		return IntStream.range(0, calls_per_table).mapToObj(i -> {
			if (i == 0)
				return testTableName(tableName);
			else if (i == 1)
				return testColumnNames(tableName, columnNames);
			else if (i == 2)
				return testColumnTypes(tableName, columnTypes);
			else if (i == 3)
				return testPrimaryIndex(tableName, primaryIndex);

			if (i == 4 || i == calls_per_table-1)
				return testClear(tableName, columnNames, columnTypes, primaryIndex);

			if (i % 20 == 0 || i == calls_per_table-2)
				return testIterator();

			var p = RNG.nextDouble();
			if (p < 0.85)
				return testPut(tableName, columnTypes, primaryIndex);
			else if (p < 0.95)
				return testRemove(tableName, columnTypes, primaryIndex);
			else
				return testGet(tableName, columnTypes, primaryIndex);
		});
	}
}