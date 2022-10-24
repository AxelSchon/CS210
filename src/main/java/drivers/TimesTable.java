package drivers;

import static sql.FieldType.INTEGER;

import java.util.ArrayList;
import java.util.LinkedList;
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
 * TIMES TABLE 4
 *   -> result set:
 *     schema:
 * 	     x INTEGER PRIMARY
 *       x2 INTEGER
 *       x3 INTEGER
 *       x4 INTEGER
 *	   state:
 *       [1, 2,  3,  4]
 *       [2, 4,  6,  8]
 *       [3, 6,  9, 12]
 *       [4, 8, 12, 16]
 *
 * TIMES TABLE 4 AS num
 *   -> result set:
 *     schema:
 * 	     num INTEGER PRIMARY
 *       num_x2 INTEGER
 *       num_x3 INTEGER
 *       num_x4 INTEGER
 *     state:
 *       see TIMES TABLE 4
 *
 * TIMES TABLE 3 BY 5
 *   -> result set:
 *     schema:
 * 	     x PRIMARY INTEGER
 *       x2 INTEGER
 *       x3 INTEGER
 *       x4 INTEGER
 *       x5 INTEGER
 *	   state:
 *       [1, 2,  3,  4,  5]
 *       [2, 4,  6,  8, 10]
 *       [3, 6,  9, 12, 15]
 *
 * TIMES TABLE 3 BY 5 AS val
 *   -> result set:
 *     schema:
 * 	     val INTEGER PRIMARY
 *       val_x2 INTEGER
 *       val_x3 INTEGER
 *       val_x4 INTEGER
 *       val_x5 INTEGER
 *	   state:
 *       see TIMES TABLE 3 BY 5
 */
@Deprecated
public class TimesTable implements Driver {
	private static final Pattern pattern = Pattern.compile(
			// TIMES\s+TABLE\s+([0-9]+)(?:\s+by\s+([0-9]+))?(?:\s+AS\s+([a-z][a-z0-9_]*))?
			"TIMES\\s+TABLE\\s+([0-9]+)(?:\\s+by\\s+([0-9]+))?(?:\\s+AS\\s+([a-z][a-z0-9_]*))?",
			Pattern.CASE_INSENSITIVE);

	private int numRows; // number of rows specified by by user. Also determines numCols if number of columns is not inputed. 
	private int numCols; // number of cols specified by user. (Optional)
	private String name; // name of corresponding types. (Optional)

	@Override
	public boolean parse(String query) throws QueryError {
		Matcher matcher = pattern.matcher(query.strip());
		// if matcher doesn't match, return false
		if (!matcher.matches())
			return false;

		// make sure user input values which are within the bounds of int
		try {
			numRows = Integer.parseInt(matcher.group(1));
			if (matcher.group(2) != null)
				numCols = Integer.parseInt(matcher.group(2));
			else
				numCols = numRows;
		} catch (NumberFormatException e) {
			throw new QueryError("Integers must be within signed 32-bit bounds", e);
		}

		// make sure user input a legal number of rows/cols
		if (Integer.parseInt(matcher.group(1)) < 1 || Integer.parseInt(matcher.group(1)) > 15)
			throw new QueryError("Number of rows must be greater than 0 and less than 16");
		if (matcher.group(2) != null
				&& (Integer.parseInt(matcher.group(2)) < 1 || Integer.parseInt(matcher.group(2)) > 15))
			throw new QueryError("Number of columns must be greater than 0 and less than 16");

		// if user input a name, store it
		if (matcher.group(3) == null) // user did not input name. 
			name = null; // no name specified 
		else if (matcher.group(3).length() > 11) // check to make sure length of group 2 is within bounds. 
			throw new QueryError("A name must be 1 to 11 characters");
		else // user did input name
			name = matcher.group(3); // user inputed name

		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {
		// store column names into ArrayList
		ArrayList<String> names = new ArrayList<String>();
		for (int i = 1; i <= numCols; i++) {
			if (name != null) { // if user did not input a name
				if (i == 1) // for first column header
					names.add(name); // store only the default column header
				else // for all other column headers
					names.add(name + "_x" + i); // store the default column header followed by the column number
			} else { // if user input a name
				if (i == 1) // for first column header
					names.add("x"); // only add the name
				else // for all other column headers
					names.add("x" + i); // store name followed by column number
			}
		}

		// store column types into ArrayList
		ArrayList<FieldType> types = new ArrayList<FieldType>();
		for (int i = 0; i < numCols; i++) {
			types.add(INTEGER); // FieldType will always be of type INTEGER for TimesTable
		}

		Table resultSet = new HashArrayTable("_times", names, types, 0);

		for (int i = 1; i <= numRows; i++) { // for each row
			List<Object> row = new LinkedList<>(); // create a new row
			for (int j = 1; j <= numCols; j++) { // for each column in that row
				row.add(j * i); // add the current row multiplied by the current column to current column. 
			}
			resultSet.put(row); // add the row to the result set
		}

		return resultSet;
	}
}
