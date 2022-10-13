package drivers;

import static sql.FieldType.BOOLEAN;
import static sql.FieldType.INTEGER;
import static sql.FieldType.STRING;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import sql.Driver;
import sql.QueryError;
import tables.SearchTable;
import tables.Table;

/**
 * Driver for execution of named macros
 * for testing or grading purposes.
 * <p>
 * Modify the macros for your use case.
 */
@Deprecated
public class Macros implements Driver {
	private static final Pattern pattern = Pattern.compile("MACRO\\s+(\\w+)(?:\\s+(.+))?", Pattern.CASE_INSENSITIVE);

	private String macro;
	private String option;

	@Override
	public boolean parse(String query) {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		macro = matcher.group(1).strip().toUpperCase();
		if (matcher.group(2) != null)
			option = matcher.group(2).strip();
		else
			option = null;

		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {
		return switch (macro) {
		case "1" -> {
			/*
			 * MACRO 1
			 *   -> table: macro_1 with 3 columns by 8 rows
			 *     (table also injected into database)
			 */

			Table table = new SearchTable("macro_1", List.of("letter", "order", "vowel"),
					List.of(STRING, INTEGER, BOOLEAN), 0);
			table.put(List.of("alpha", 1, true));
			table.put(List.of("beta", 2, false));
			table.put(List.of("gamma", 3, false));
			table.put(List.of("delta", 4, false));
			table.put(List.of("tau", 19, false));
			table.put(List.of("pi", 16, false));
			table.put(List.of("omega", 24, true));
			table.put(Arrays.asList("N/A", null, null));

			db.create(table);

			yield table;
		}
		case "2" -> {
			/*
			 * MACRO 2
			 *   -> result set: macro_2 renamed from nested query
			 *     (result set not injected into database)
			 */

			Table resultRet = (Table) db.interpret("RANGE 10 AS x");
			resultRet.setTableName("macro_2");

			yield resultRet;
		}
		case "SIZEOF" -> {
			/*
			 * MACRO SIZEOF macro_1
			 *   -> integer (affected rows): 8
			 */

			Table table = (Table) db.interpret("SHOW TABLE %s".formatted(option));

			yield table.size();
		}
		case "RESET" -> {
			/*
			 * MACRO RESET
			 *   -> integer (affected rows): varies
			 *     (all tables dropped from database)
			 */

			int sum = 0;

			for (Table table : db.tables()) {
				sum += table.size();
				db.drop(table.getTableName());
			}

			yield sum;
		}

		// below are my personal macros
		case "TABLE1" -> {
			/*
			 * MACRO 1
			 *   -> table: macro_1 with 3 columns by 8 rows
			 *     (table also injected into database)
			 */

			Table table = new SearchTable("TABLE1", List.of("letter", "order", "vowel"),
					List.of(STRING, INTEGER, BOOLEAN), 0);
			table.put(List.of("a", 1, true));
			table.put(List.of("b", 2, false));
			table.put(List.of("g", 3, false));
			table.put(List.of("d", 4, true));
			table.put(List.of("t", 5, false));
			table.put(List.of("p", 6, false));
			table.put(List.of("o", 7, true));
			table.put(Arrays.asList("N/A", null, null));

			db.create(table);

			yield table;
		}
		case "TABLE2" -> {
			/*
			 * MACRO 1
			 *   -> table: macro_1 with 3 columns by 8 rows
			 *     (table also injected into database)
			 */

			Table table = new SearchTable("TABLE2", List.of("letter", "order", "vowel", "value"),
					List.of(STRING, INTEGER, BOOLEAN, INTEGER), 1);
			table.put(List.of("alpha", 10000, true, 300));
			table.put(List.of("beta", 500, false, 200));
			table.put(List.of("gamma", 250, false, 100));
			table.put(List.of("delta", 125, false, 50));
			table.put(List.of("tau", 62, false, 25));
			table.put(List.of("pi", 31, false, 15));
			table.put(List.of("omega", 15, true, 5));
			table.put(Arrays.asList("N/A", 7, null, null));

			db.create(table);

			yield table;
		}
		case "TABLE3" -> {
			Table table = new SearchTable("TABLE3", List.of("letter", "order", "vowel", "int", "boolean"),
					List.of(STRING, INTEGER, BOOLEAN, INTEGER, BOOLEAN), 3);
			table.put(List.of("this", 1, true, 0, false));
			table.put(List.of("is", 2, false, 1, false));
			table.put(List.of("another", 3, false, 2, false));
			table.put(List.of("table", 4, true, 3, false));
			table.put(List.of("for", 5, false, 4, false));
			table.put(List.of("demonstrational", 6, false, 5, false));
			table.put(List.of("purposes", 7, true, 6, false));
			table.put(Arrays.asList("this string is too long to display", null, true, 7, false));

			db.create(table);

			yield table;
		}

		default -> {
			throw new QueryError("Macro <%s> is undefined".formatted(macro));
		}
		};
	}
}
