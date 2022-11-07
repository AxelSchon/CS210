package drivers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import sql.Driver;
import sql.QueryError;

/*
 * SHOW TABLE example_table
 * 	 -> table: example_table from database
 */
public class InsertInto implements Driver {
	private static final Pattern queryPattern = Pattern.compile(
			// sanitizes the keywords, table name, column names, stuff (for literals)
			// group 1: keyword
			// group 2: table name
			// group 3: column names list (or null)
			// group 4: literal values list 
			"escaped pattern", Pattern.CASE_INSENSITIVE);
	private static final Pattern literalPattern = Pattern.compile(
			// Literal Pattern
			"escaped pattern", Pattern.CASE_INSENSITIVE);

	// field for whether insert or replace mode (boolean flag)
	// field for tablename
	// field for whether short/long form (boolean flag)
	// field for column names (array of strings)
	// field for literal values (array of strings)

	@Override
	public boolean parse(String query) throws QueryError {
		Matcher matcher = queryPattern.matcher(query.strip());
		if (!matcher.matches())
			return false;
		// assign a mode flag equal to whether or not
		// the INSERT keyword was used (Vs Replace keyword)
		// eg. insertMode, requiredUniqueKeys, isInsert
		// eg. replaceMode, allowDuplicateKeys, isReplace

		// get the table name

		// check if group 3 is null:
		//	 set the corresponding short/long form flag to show that it's short form
		// otherwise:
		//	 set the short/long form flag to show that it's long form
		//	 split group 3 on commas into a field for the column names

		// split group 4 on commas into a field for the literal values
		// eg. ["A", "1", "true"] raw characters for each
		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {
		// PHASE 1 - find a correspondence between
		// col names in query and col names in the schema

		// if short form"
		//	 build the pointers lst to exactly match the schema
		//		 for each index that the pointers list should have:
		//			 add that index as the element ot the end of the pointers list
		// else if long form:
		//	 build the pointers list with the correspondence form the query 
		//		 for each query column name:
		//			 j (real schema index) = ask the schema's col names list for the indexOf the col name
		//			 if  was -1: throw a query error (such as unknown column name)
		//			 if j is already contained in the pointers list: throw a query error (such as duplicate column)
		//			 add the j index to the end of the pointers list		
		// if primary index is not contained in the pointers list: throw a query error

		// PHASE 2 - build the row to be inserted/replaced
		// based on the correspondence already found
		return null;
	}
}
