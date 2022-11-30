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
import tables.HashArrayTable;

public class SelectFrom implements Driver {

	//constants
	static final int MAX_STRING_LENGTH = 127;

	private static final Pattern queryPattern = Pattern.compile(
			// SELECT\s+(\*\s+|\s*[a-z][a-z0-9_]*(?:\s+AS\s+[a-z][a-z0-9_]*)?(?:\s*,\s*[a-z][a-z0-9_]*(?:\s+AS\s+[a-z][a-z0-9_]*)?)*\s*)(?:FROM\s+([a-z][a-z0-9_]*))(?:\s+WHERE\s+([a-z][a-z0-9_]*)\s*(<=|>=|<>|<|>|=)\s*([^<=>]*))?
			"SELECT\\s+(\\*\\s+|\\s*[a-z][a-z0-9_]*(?:\\s+AS\\s+[a-z][a-z0-9_]*)?(?:\\s*,\\s*[a-z][a-z0-9_]*(?:\\s+AS\\s+[a-z][a-z0-9_]*)?)*\\s*)(?:FROM\\s+([a-z][a-z0-9_]*))(?:\\s+WHERE\\s+([a-z][a-z0-9_]*)\\s*(<=|>=|<>|<|>|=)\\s*([^<=>]*))?",
			Pattern.CASE_INSENSITIVE);
	public static final Pattern literalPattern = InsertInto.literalPattern;

	boolean starMode;// field for star mode or not (boolean flag)
	String tableName; // field for tablename
	String lhsColumnName; // field for lhs column name
	String operator; // field for operator
	String rhsLiteralValue; // field for rhs literal value (not yet sanitized)
	List<String> columnNames; // field for column names or aliases (array of strings) 

	@Override
	public boolean parse(String query) throws QueryError {
		Matcher matcher = queryPattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		// initialize starMode
		if (matcher.group(1).strip().equals("*")) {
			starMode = true;
		} else {
			starMode = false;
			columnNames = Arrays.asList(matcher.group(1).split("\\s*,\\s*"));
		}

		tableName = matcher.group(2);
		lhsColumnName = matcher.group(3);
		operator = matcher.group(4);
		rhsLiteralValue = matcher.group(5);

		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {
		HashArrayTable select;

		// if the table doesn't exist in the database:
		if (!db.exists(tableName)) {
			// throw an error
			throw new QueryError("Table name already exists within database");
		}

		var sourceTable = db.find(tableName);

		// define pointers list
		ArrayList<Integer> pointerList = new ArrayList<Integer>();
		//define result set schema
		ArrayList<String> rsSchema = new ArrayList<String>();

		// define table information from schema
		var schemaColumnNames = db.find(tableName).getColumnNames();
		var schemaColumnTypes = db.find(tableName).getColumnTypes();
		var schemaPrimaryIndex = db.find(tableName).getPrimaryIndex();

		// PHASE 1 - find a correspondence between col names in query and col names in the schema

		// if short form
		if (starMode == true) {
			// build the pointers lst to exactly match the schema
			for (int i = 0; i < schemaColumnNames.size(); i++) { // for each index that the pointers list should have:
				pointerList.add(i); // add that index as the element to the end of the pointers list
			}
			select = new HashArrayTable("_select", schemaColumnNames, schemaColumnTypes, schemaPrimaryIndex);
		} else { // else if long form:
			// initialize column names / column types list for _select
			ArrayList<String> rsSchemaColumnNames = new ArrayList<String>();
			ArrayList<FieldType> rsSchemaColumnTypes = new ArrayList<FieldType>();
			int rsSchemaPrimaryIndex = -1; // initialize result set schema primary index to -1

			// build the pointers list with the correspondence from the query 
			for (int i = 0; i < columnNames.size(); i++) { // for each query column name/alias:	
				String[] nameAndAlias = columnNames.get(i).split("\\s+"); // split column names and alias into separate column name and alias. 

				// save column name and alias
				String name;
				String alias;
				if ((nameAndAlias.length == 3) && (nameAndAlias[1].equalsIgnoreCase("as"))) {
					name = nameAndAlias[0].strip();
					alias = nameAndAlias[2].strip();
				} else {
					name = nameAndAlias[0].strip();
					alias = nameAndAlias[0].strip();
				}

				int j = schemaColumnNames.indexOf(name); // j (real schema index) = ask the schema's col names list for the indexOf the col name

				// if  was -1: throw a query error (such as unknown column name)
				if (j == -1)
					throw new QueryError("Unknown Column Name");

				// if the alias is already contained in the result set schema column names list: throw a query error 
				if (rsSchemaColumnNames.contains(alias))
					throw new QueryError("Duplicate column name");

				pointerList.add(j);// add the j index to the end of the pointers list
				rsSchemaColumnNames.add(alias); // add the alias to the column names list were building
				rsSchemaColumnTypes.add(schemaColumnTypes.get(j)); // add the type to the column types list were building

				// if first primary seen, update the primary index
				if ((j == schemaPrimaryIndex) && (rsSchemaPrimaryIndex == -1)) {
					rsSchemaPrimaryIndex = i;
				}
			}

			// if primary index is not contained in the pointers list: throw a query error
			if (!pointerList.contains(db.find(tableName).getPrimaryIndex()) || rsSchemaPrimaryIndex == -1)
				throw new QueryError("Primary index not specified");

			// build result set based off sanitized data above
			select = new HashArrayTable("_select", rsSchemaColumnNames, rsSchemaColumnTypes, rsSchemaPrimaryIndex);
		}

		// PHASE 2 - build the row to be inserted/replaced

		// goal variables
		int lhsColumnIndex; // LHS column Index
		FieldType lhsColumnType;
		FieldType rhsLiteralType;
		Object resultingValue = null;

		if (operator == null) { // where clause missing
			lhsColumnIndex = 0;
			lhsColumnType = null;
			rhsLiteralType = null;
		} else {
			if (schemaColumnNames.contains(lhsColumnName)) {
				lhsColumnIndex = schemaColumnNames.indexOf(lhsColumnName);
				lhsColumnType = schemaColumnTypes.get(lhsColumnIndex);
			} else {
				throw new QueryError("specified column name does not exist in the source table");
			}

			Matcher matcher = literalPattern.matcher(rhsLiteralValue);
			if (!matcher.matches())
				throw new QueryError("literal doesn't match any of the available types");

			// if the literal is a string
			if ((matcher.group("string") != null)) { // string

				// check string length
				if (matcher.group("string").length() > MAX_STRING_LENGTH)
					throw new QueryError("String name too long");

				resultingValue = matcher.group("string");
				rhsLiteralType = FieldType.STRING;

				// if the literal is an int
			} else if ((matcher.group("int") != null)) {

				// check to make sure int is within bounds
				try {
					Integer.parseInt(matcher.group("int"));
				} catch (NumberFormatException e) {
					throw new QueryError("Integers must be within signed 32-bit bounds", e);
				}

				resultingValue = Integer.parseInt(matcher.group("int"));
				rhsLiteralType = FieldType.INTEGER;

				// if the literal is a boolean
			} else if ((matcher.group("bool") != null)) {

				resultingValue = Boolean.parseBoolean(matcher.group("bool"));
				rhsLiteralType = FieldType.BOOLEAN;

				// otherwise the literal is a null
			} else {
				resultingValue = null;
				rhsLiteralType = null;
			}
		}

		// PHASE 3
		for (var row : sourceTable) {
			boolean selectFlag = true; // whether the row should be selected or not

			//if there is an operator (if there was a where clause)
			if (operator != null) {
				int compareValue = 0;

				// v How do I get the element/row from the table? aka the correct lhsValue.
				Object lhsValue = row.get(lhsColumnIndex); // lhs value that corresponds to the lhs index
				if (lhsValue == null || rhsLiteralValue.equalsIgnoreCase("null")) { // special case for null
					selectFlag = false;
				} else { // not special case
					if (lhsColumnType.equals(rhsLiteralType)) { // both lhs and rhs are same type
						if (lhsColumnType.equals(FieldType.STRING)) { // both are STRING type
							compareValue = ((String) lhsValue).compareTo((String) resultingValue);
						} else if (lhsColumnType.equals(FieldType.INTEGER)) { // both are INTEGER type
							compareValue = ((Integer) lhsValue).compareTo((Integer) resultingValue);
						} else if (lhsColumnType.equals(FieldType.BOOLEAN)) { // both are INTEGER type
							compareValue = ((Boolean) lhsValue).compareTo((Boolean) resultingValue);
						}

					} else if (!lhsColumnType.equals(rhsLiteralType)) { // lhs type is different from rhs type
						compareValue = lhsValue.toString().compareTo(resultingValue.toString());
					}

					if (operator.equals("=")) {
						selectFlag = compareValue == 0;
					} else if (operator.equals("<>")) {
						selectFlag = compareValue != 0;
					} else if (operator.equals("<")) {
						selectFlag = compareValue < 0;
					} else if (operator.equals(">")) {
						selectFlag = compareValue > 0;
					} else if (operator.equals("<=")) {
						selectFlag = compareValue <= 0;
					} else if (operator.equals(">=")) {
						selectFlag = compareValue >= 0;
					}
				}
			}

			//if selected, build and add the row
			if (selectFlag == true) {
				ArrayList<Object> selectedRow = new ArrayList<Object>();
				for (int j = 0; j < pointerList.size(); j++) {
					var element = row.get(pointerList.get(j));
					selectedRow.add(element);
				}
				select.put(selectedRow);
			}
		}

		return select;
	}
}
