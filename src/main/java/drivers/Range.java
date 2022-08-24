package drivers;

import static sql.FieldType.INTEGER;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import sql.Driver;
import sql.QueryError;
import tables.SearchTable;
import tables.Table;

/*
 * RANGE 5
 *   -> result set:
 *     schema:
 * 	     number INTEGER PRIMARY
 *     state:
 *	   	 [0]
 *	   	 [1]
 *	   	 [2]
 *	   	 [3]
 *	   	 [4]
 *
 * RANGE 5 AS x
 *   -> result set:
 *     schema:
 * 	     x INTEGER PRIMARY
 *     state:
 *       see RANGE 5
 */
public class Range implements Driver {
	private static final Pattern pattern = Pattern.compile(
		"RANGE\\s+([0-9]+)(?:\\s+AS\\s+([a-z][a-z0-9_]*))?",
		Pattern.CASE_INSENSITIVE
	);

	private int upper;
	private String name;

	@Override
	public boolean parse(String query) throws QueryError {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		try {
			upper = Integer.parseInt(matcher.group(1));
		}
		catch (NumberFormatException e) {
			throw new QueryError("Integers must be within signed 32-bit bounds", e);
		}

		if (matcher.group(2) == null)
			name = "number";
		else if (matcher.group(2).length() > 15)
			throw new QueryError("A column name must be 1 to 15 characters");
		else
			name = matcher.group(2);

		return true;
	}

	@Override
	public Object execute(Database db) {
		Table resultSet = new SearchTable(
			"_range",
			List.of(name),
			List.of(INTEGER),
			0
		);

		for (int i = 0; i < upper; i++) {
			List<Object> row = new LinkedList<>();
			row.add(i);
			resultSet.put(row);
		}

		return resultSet;
	}
}
