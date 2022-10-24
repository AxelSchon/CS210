package drivers;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import sql.Driver;
import sql.FieldType;
import sql.QueryError;

/*
 * SHOW TABLE example_table
 * 	 -> table: example_table from database
 */
public class CreateTable implements Driver {
	private static final Pattern pattern = Pattern.compile(
			// 
			"", Pattern.CASE_INSENSITIVE);

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
		columnNames = new ArrayList<String>(); // initialize columnNames to an empty list
		columnTypes = new ArrayList<FieldType>(); // initialize columnTypes to a new empty list
		primaryIndex = -1; // initialize primaryIndex to be -1 (fake index)

		// colDefs arrays is result of splitting group 2 on commas

		// for each colDef in colDefs array:
		{
			// strip and split on whitespace
			// guaranteed to have either 2 or 3 words

			// if 3 words and the word at index 2 is PRIMARY:
			//	 if this is NOT the first time (so p.i. is NOT still -1):
			//		 throw an error
			//	 update the p.i. to be the index of this column

			// if column name (0th element of colDef) is too long:
			//	 throw and error

			// if the column name is already contained in the columnmNames list: (list.contains method)
			//	 throw an error

			// add the name onto the end of the columnNames list

			// add the type (1st element of colfDef) to the columnTypes list
			//	 HINT: check FieldType API. Find what the type should be based on string user identified. (Should be a one line operation)

			// if columnNames list is too long:
			//	 throw an error
		}

		// if the primaryIndex was never initialized (still -1):
		//	 throw an error

		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {

		// TODO: similar to other drivers

		// if tableName already exists in db:
		//	 throw an error

		// create the table with the 4 schema properties:
		//	 if the db is persistent (and you ahve a working M2):
		//		 when creating table, use the HashFile type
		// otherwise:
		//	 when creating table, use the HashArrayTable type

		// tell the db to create/add that table

		//return the table itself

		return null;
	}
}
