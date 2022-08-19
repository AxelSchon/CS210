package drivers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import sql.Driver;
import sql.QueryError;
import tables.Table;

/*
 * SHOW TABLE example_table
 * 	 -> table: example_table from database
 */
public class ShowTable implements Driver {
	private static final Pattern pattern = Pattern.compile(
		"SHOW\\s+TABLE\\s+([a-z][a-z0-9_]*)",
		Pattern.CASE_INSENSITIVE
	);

	private String tableName;

	@Override
	public boolean parse(String query) {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		tableName = matcher.group(1);

		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {
		Table table = db.find(tableName);

		if (table == null)
			throw new QueryError("Missing table <%s>".formatted(tableName));

		return table;
	}
}
