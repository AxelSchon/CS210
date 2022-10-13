package apps;

import static sql.FieldType.BOOLEAN;
import static sql.FieldType.INTEGER;
import static sql.FieldType.STRING;

import java.util.Arrays;
import java.util.List;

import tables.SearchTable;
import tables.Table;

/**
 * Sandbox for execution of arbitrary code
 * for testing or grading purposes.
 * <p>
 * Modify the code for your use case.
 */
@Deprecated
public class Sandbox {
	public static void main(String[] args) {
		Table table = new SearchTable("sandbox_1", List.of("letter", "o", "vowel"), List.of(STRING, INTEGER, BOOLEAN),
				0);

		table.put(List.of("alpha", 1, true));
		table.put(List.of("beta", 2, false));
		table.put(List.of("gamma", 3, false));
		table.put(List.of("delta", 4, false));
		table.put(List.of("tau", 19, false));
		table.put(List.of("pi", 16, false));
		table.put(List.of("omega", 24, true));
		table.put(Arrays.asList("N/A", null, null));

		System.out.println(table);

		Table table2 = new SearchTable("sandbox_1", List.of("letter", "order", "vowel", "value"),
				List.of(STRING, INTEGER, BOOLEAN, INTEGER), 1);

		table2.put(List.of("01234567890123456789012", 1, true, 1));
		table2.put(List.of("beta", 2, false, 2));
		table2.put(List.of("gamma", 3, false, 3));
		table2.put(List.of("delta", 4, false, 4));
		table2.put(List.of("tau", 19, false, 5));
		table2.put(List.of("pi", 16, false, 6));
		table2.put(List.of("omega", 24, true, 7));
		table2.put(Arrays.asList(null, 34, null, 8));

		System.out.println(table2);
	}
}
