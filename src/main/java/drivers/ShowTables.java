package drivers;

import static sql.FieldType.INTEGER;
import static sql.FieldType.STRING;

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

		return matcher.matches(); // true if matches, otherwise false.
	}

	@Override
	public Object execute(Database db) {

		// create table as per requirements in per project document
		Table resultSet = new SearchTable("_tables", List.of("table_name", "column_count", "row_count"),
				List.of(STRING, INTEGER, INTEGER), 0);

		// for each table in the database's list of tables:
		for (Table table : db.tables()) {
			// add that tables tableName, col_count, and row_count to the table _tables
			resultSet.put(List.of(table.getTableName(), table.getColumnNames().size(), table.size()));
		}

		return resultSet;
	}
}
