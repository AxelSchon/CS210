package drivers;

import static sql.FieldType.INTEGER;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import sql.Driver;
import sql.QueryError;
import tables.SearchTable;
import tables.Table;

public class ShowTables implements Driver {
	private static final Pattern pattern = Pattern.compile(
			// SHOW\s+TABLES
			"SHOW\\s+TABLES", Pattern.CASE_INSENSITIVE);

	@Override
	public boolean parse(String query) throws QueryError {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		return true;

		// TODO: convert lines 24 to 27 into a single return statement
	}

	@Override
	public Object execute(Database db) {
		Table resultSet = new SearchTable(
				// TODO: Update schema based on requirements (un-hard-code List.of)
				"_range", List.of("asdf"), List.of(INTEGER), 0);

		// for each table in the database's list of tables:
		// (enhanced for loop)
		// { 
		//List<Object> row = new LinkedList<>();
		//row.add(i); // name of table, get from table's schema
		//row.add(i); // # columns, getColumnNames.size()
		//row.add(i); // # rows, same technique as DropTables return
		//resultSet.put(row);
		// }

		return resultSet;
	}
}
