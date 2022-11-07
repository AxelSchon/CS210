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

	// define fields
	private String tableName;

	@Override
	public boolean parse(String query) {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		// initialize field
		tableName = matcher.group(1);

		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {

		// drop table based on tableName
		Table table = db.drop(tableName);

		// if the table was not dropped because it doesn't exist
		if (table == null)
			// throw an error
			throw new QueryError("Missing table <%s>".formatted(tableName));

		// return the number of rows dropped
		return table.size();
	}
}
