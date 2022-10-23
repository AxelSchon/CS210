package drivers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import sql.Driver;
import sql.QueryError;
import tables.Table;

public class DropTable implements Driver {
	private static final Pattern pattern = Pattern.compile(
			// DROP\s+TABLE\s+([a-z][a-z0-9_]*)
			"DROP\\s+TABLE\\s+([a-z][a-z0-9_]*)", Pattern.CASE_INSENSITIVE);

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
		Table table = db.drop(tableName);

		if (table == null)
			throw new QueryError("Missing table <%s>".formatted(tableName));

		// TODO (optional)
		// if the database is persistent
		// then if the table happens to be a hash file table
		// then do anything necessary to clean it up

		return table.size();
	}
}
