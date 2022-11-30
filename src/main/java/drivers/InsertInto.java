package drivers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import sql.Driver;
import sql.FieldType;
import sql.QueryError;

public class InsertInto implements Driver {

	//constants
	static final int MAX_STRING_LENGTH = 127;

	private static final Pattern queryPattern = Pattern.compile(
			// sanitizes the keywords, table name, column names, stuff (for literals)
			// group 1: keyword
			// group 2: table name
			// group 3: column names list (or null)
			// group 4: literal values list

			// two medium-ish patterns
			//	 (INSERT|REPLACE)\s+INTO\s+([a-z][a-z0-9_]*)\s+(?:\((\s*[a-z][a-z0-9_]*(?:\s*,\s*(?:[a-z][a-z0-9_]*)*\s*)*)\)\s+)?VALUES\s+(?:\(([^()]*)\))
			//	 "([^"]*)"|(NULL)|(TRUE|FALSE)|((?:[+]?|[-]?)\d*)

			"(?<keyword>INSERT|REPLACE)\\s+INTO\\s+(?<tableName>[a-z][a-z0-9_]*)\\s+(?:\\((?<columnNames>\\s*[a-z][a-z0-9_]*(?:\\s*,\\s*(?:[a-z][a-z0-9_]*)*\\s*)*)\\)\\s+)?VALUES\\s+(?:\\((?<literalValues>[^()]*)\\))",
			Pattern.CASE_INSENSITIVE);
	public static final Pattern literalPattern = Pattern.compile(
			// Literal Pattern
			// "([^"]*)"|(NULL)|(TRUE|FALSE)|((?:[+]?|[-]?)\d*)
			"\"(?<string>[^\"]*)\"|(NULL)|(?<bool>TRUE|FALSE)|(?<int>(?:[+]?|[-]?)(?:0|[1-9]\\d*))",
			Pattern.CASE_INSENSITIVE);

	boolean insertMode;// field for whether insert or replace mode (boolean flag)
	String tableName; // field for tablename
	boolean shortForm; // field for whether short/long form (boolean flag)
	List<String> columnNames; // field for column names (array of strings)
	List<String> literalValues;	// field for literal values (array of strings)

	@Override
	public boolean parse(String query) throws QueryError {
		Matcher matcher = queryPattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		insertMode = matcher.group("keyword").equalsIgnoreCase("insert"); // assign a mode flag equal to whether or not the INSERT keyword was used (vs Replace keyword)
		tableName = matcher.group("tableName");// get the table name

		// check if group 3 (columnNames) is null:
		if (matcher.group("columnNames") == null) {
			shortForm = true; // set the corresponding short/long form flag to show that it's short form
		} else {// otherwise:
			shortForm = false; // set the short/long form flag to show that it's long form
			columnNames = Arrays.asList(matcher.group("columnNames").split("\\s*,\\s*")); // split group 3 on commas into a field for the column names
		}

		// split group 4 (literalValues) on commas into a field for the literal values
		literalValues = Arrays.asList(matcher.group("literalValues").split("\\s*,\\s*")); // eg. ["A", "1", "true"] raw characters for each

		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {
		// if the table doesn't exist in the database:
		if (!db.exists(tableName)) {
			// throw an error
			throw new QueryError("Table name already exists within database");
		}

		// define pointers list
		ArrayList<Integer> pointerList = new ArrayList<Integer>();

		// define columns form schema
		var schemaColumnNames = db.find(tableName).getColumnNames();
		var schemaColumnTypes = db.find(tableName).getColumnTypes();

		// PHASE 1 - find a correspondence between col names in query and col names in the schema

		// if short form
		if (shortForm == true) {
			// build the pointers lst to exactly match the schema
			for (int i = 0; i < schemaColumnNames.size(); i++) { // for each index that the pointers list should have:
				pointerList.add(i); // add that index as the element to the end of the pointers list
			}
		} else { // else if long form:
			// build the pointers list with the correspondence form the query 
			for (int i = 0; i < columnNames.size(); i++) { // for each query column name:				
				int j = schemaColumnNames.indexOf(columnNames.get(i)); // j (real schema index) = ask the schema's col names list for the indexOf the col name

				// if  was -1: throw a query error (such as unknown column name)
				if (j == -1)
					throw new QueryError("Unknown Column Name");

				// if j is already contained in the pointers list: throw a query error (such as duplicate column)
				if (pointerList.contains(j))
					throw new QueryError("Duplicate column");

				pointerList.add(j);// add the j index to the end of the pointers list
			}

			// if primary index is not contained in the pointers list: throw a query error
			if (!pointerList.contains(db.find(tableName).getPrimaryIndex()))
				throw new QueryError("Primary index not specified");
		}

		// PHASE 2 - build the row to be inserted/replaced

		// if the number of literals doesn't match the number of columns in query (or pointers list)
		if (literalValues.size() != pointerList.size())
			throw new QueryError("Number of literals doesn't match number of columns in query");

		// make a new empty row and fill it with nulls up to the number of columns in the schema 
		int rowsAdded = 0;
		ArrayList<Object> row = new ArrayList<Object>();
		for (int i = 0; i < schemaColumnNames.size(); i++)
			row.add(null);

		// for each index i (for each name that comes from the query)
		for (int i = 0; i < pointerList.size(); i++) {
			int j = pointerList.get(i); // j = the element at pointerList[i]
			var type = schemaColumnTypes.get(j); // column type from schema
			String literal = literalValues.get(i); // literal from query
			Object resultingValue = null; // uninitialized resulting value

			Matcher matcher = literalPattern.matcher(literal);
			if (!matcher.matches())
				throw new QueryError("literal doesn't match any of the available types");

			// if the literal is a string
			if ((matcher.group("string") != null)) { // string

				// check string length
				if (matcher.group("string").length() > MAX_STRING_LENGTH)
					throw new QueryError("String name too long");

				// check if literal type matches the schema type
				if (type != FieldType.STRING)
					throw new QueryError("mismatch");

				resultingValue = matcher.group("string");

				// if the literal is an int
			} else if ((matcher.group("int") != null)) {

				// check if literal type matches the schema type
				if (type != FieldType.INTEGER)
					throw new QueryError("mismatch");

				// check to make sure int is within bounds
				try {
					Integer.parseInt(matcher.group("int"));
				} catch (NumberFormatException e) {
					throw new QueryError("Integers must be within signed 32-bit bounds", e);
				}

				resultingValue = Integer.parseInt(matcher.group("int"));

				// if the literal is a boolean
			} else if ((matcher.group("bool") != null)) {

				// check if literal type matches the schema type				
				if (type != FieldType.BOOLEAN)
					throw new QueryError("mismatch");

				resultingValue = Boolean.parseBoolean(matcher.group("bool"));

				// otherwise the literal is a null
			} else {
				resultingValue = null;
			}

			// if were looking at the primary index
			if (j == db.find(tableName).getPrimaryIndex()) {

				// check to make sure the key is not null
				if (resultingValue == null)
					throw new QueryError("Primary index may not contain null");

				// check to make sure that the table does not already contain the value if not in replace mode
				if (insertMode && db.find(tableName).contains(resultingValue))
					throw new QueryError("Cannot insert duplicates in insert mode");
			}
			row.set(j, resultingValue);
		}

		db.find(tableName).put(row);
		rowsAdded++;

		return rowsAdded;
	}
}
