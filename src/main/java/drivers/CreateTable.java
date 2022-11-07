package drivers;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import sql.Driver;
import sql.FieldType;
import sql.QueryError;
import tables.HashArrayTable;
import tables.Table;

/*
 * SHOW TABLE example_table
 * 	 -> table: example_table from database
 */
public class CreateTable implements Driver {
	private static final Pattern pattern = Pattern.compile(
			// CREATE\s+TABLE\s+([a-z][a-z0-9_]*)\s+\((\s*[a-z][a-z0-9_]*\s+(?:STRING|INTEGER|BOOLEAN)(?:\s+PRIMARY)?(?:\s*,\s*(?:[a-z][a-z0-9_]*\s+(?:STRING|INTEGER|BOOLEAN)(?:\s+PRIMARY)?))*\s*)\)
			"CREATE\\s+TABLE\\s+([a-z][a-z0-9_]*)\\s+\\((\\s*[a-z][a-z0-9_]*\\s+(?:STRING|INTEGER|BOOLEAN)(?:\\s+PRIMARY)?(?:\\s*,\\s*(?:[a-z][a-z0-9_]*\\s+(?:STRING|INTEGER|BOOLEAN)(?:\\s+PRIMARY)?))*\\s*)\\)",
			Pattern.CASE_INSENSITIVE);

	// define fields
	private String tableName;
	private List<String> columnNames;
	private List<FieldType> columnTypes;
	private int primaryIndex;

	@Override
	public boolean parse(String query) throws QueryError {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		// initialize fields
		tableName = matcher.group(1);
		if (tableName.length() > 15)
			throw new QueryError("A table name must be 1 to 15 characters");
		columnNames = new ArrayList<String>();  // initialize columnNames to an empty list
		columnTypes = new ArrayList<FieldType>(); // initialize columnTypes to a new empty list
		primaryIndex = -1; // initialize primaryIndex to be -1 (fake index)

		// colDefs arrays is result of striping and splitting group 2 on commas surrounded by optional whitespace. Each index will contain [col_name col_type (potentially primary assignment)]
		String[] colDefs = matcher.group(2).strip().split("\\s*,\\s*");

		// for every column defined in the query
		for (int i = 0; i < colDefs.length; i++) {

			// split on whitespace (guaranteed to have either 2 or 3 words)
			String[] colDef = colDefs[i].split("\\s+"); // colDef[0] = col_name, colDef[1] = col_type, colDef[3] = Primary. Where colDef[3] may or may not be defined.

			// if 3 words and the word at index 2 is PRIMARY:
			if ((colDef.length == 3) && (colDef[2].equalsIgnoreCase("primary"))) {
				//if this is NOT the first time (so p.i. is NOT still -1):
				if (primaryIndex != -1) {
					// throw an error
					throw new QueryError("Primary index has already been specified.");
				}
				// update the p.i. to be the index of this column
				primaryIndex = i;
			}

			// if column name (0th element of colDef) is too long:
			if (colDef[0].length() > 15) {
				//throw and error
				throw new QueryError("Column name is too long");
			}

			// if the column name is already contained in the columnmNames list
			if (columnNames.contains(colDef[0])) {
				// throw an error	
				throw new QueryError("Column name is already used");
			}

			// add the name onto the end of the columnNames list
			columnNames.add(colDef[0]);

			// add the type (1st element of colfDef) to the columnTypes list
			columnTypes.add(FieldType.valueOf(colDef[1].toUpperCase()));

			// if columnNames list is too long:
			if (columnNames.size() > 15) {
				// throw an error
				throw new QueryError("Too many column names defined");
			}
		}

		// if the primaryIndex was never initialized (still -1):
		if (primaryIndex == -1) {
			// throw an error
			throw new QueryError("Primary index was never defined");
		}

		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {

		// if tableName already exists in db:
		if (db.exists(tableName)) {
			// throw an error
			throw new QueryError("Table name already exists within database");
		}

		// create the table with the 4 schema properties:
		Table table = new HashArrayTable(tableName, columnNames, columnTypes, primaryIndex);

		// tell the db to create/add that table
		db.create(table);

		//return the table itself
		return table;
	}
}
