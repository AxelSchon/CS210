package drivers;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import apps.Database;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import sql.Driver;
import sql.FieldType;
import sql.QueryError;
import tables.HashArrayTable;
import tables.Table;

/*
 * SHOW TABLE example_table
 * 	 -> table: example_table from database
 */
public class Import implements Driver {
	// import\s+([a-z][a-z0-9_]*).(xml|json)(?:\s+to\s+([a-z][a-z0-9_]*))?
	private static final Pattern pattern = Pattern.compile(
			"import\\s+(?<fileName>[a-z][a-z0-9_]*).(?<fileType>xml|json)(?:\\s+to\\s+(?<tableName>[a-z][a-z0-9_]*))?",
			Pattern.CASE_INSENSITIVE);

	// define fields
	String tableName; // table name as defined in query
	String fileName; // file name as defined in query
	String fileType; // file type (Json or xml) as defined in query
	boolean jsonMode = false; // true if fileType is Json

	@Override
	public boolean parse(String query) throws QueryError {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		// initialize fields
		tableName = matcher.group("tableName");
		fileName = matcher.group("fileName");
		fileType = matcher.group("fileType");

		// set flag
		if (fileType.equalsIgnoreCase("json")) {
			jsonMode = true;
		}

		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {

		Path path = Paths.get("data", "exports", fileName); // define path to import file from

		try {
			if (jsonMode == true) { // json mode

				if (path.toFile().exists()) {

					JsonReader reader = Json.createReader(new FileInputStream(path.toFile()));
					JsonObject root_object = reader.readObject();
					reader.close();

					//SCHEMA
					// 
					JsonObject schema = root_object.getJsonObject("schema"); // get the schema object from the root object

					String table_name;
					if (tableName == null) { // if table name not defined in query
						table_name = schema.getString("table_name"); // set table name to that found in file
					} else { // else
						table_name = tableName.toString();
						// set name to given name, ensuring no duplicate table names
					}

					int tableCounter = 1;
					while (db.exists(table_name)) { // while table name exists
						if (tableCounter == 1) { // if first time finding same table
							table_name = table_name + "_" + tableCounter; // add underscore and number to represent number of times found
						} else { // have already made a table name same as one considered in if case
							table_name = table_name.substring(0, table_name.length() - 1) + tableCounter; // replace number at end to represent how many times found
						}
						tableCounter++;
					}

					JsonArray column_names_array = schema.getJsonArray("column_names"); // get column names array from schema object
					List<String> column_names = new LinkedList<>(); // define list to store column names from file
					for (int i = 0; i < column_names_array.size(); i++) { // for the number of column names in the file
						column_names.add(column_names_array.getString(i)); // store that column name to the column names list
					}

					JsonArray column_types_array = schema.getJsonArray("column_types"); // get column types array from schema object
					List<FieldType> column_types = new LinkedList<>(); // define list to store column types from file
					for (int i = 0; i < column_types_array.size(); i++) { // for the number of column types in the file
						FieldType type = FieldType.valueOf(column_types_array.getString(i)); // define column type from file
						column_types.add(type); // store that column type to the column types list
					}

					int primary_index = schema.getInt("primary_index"); // get primary index int from schema object

					Table table = new HashArrayTable(table_name, column_names, column_types, primary_index); // create table

					// STATE
					//
					JsonArray state = root_object.getJsonArray("state"); // get state array from root object

					JsonArray row_array;
					for (int i = 0; i < state.size(); i++) { // for all rows
						row_array = state.getJsonArray(i); // get row array from state array
						ArrayList<Object> elements = new ArrayList<Object>(); // create an array to hold all elements in this row
						for (int j = 0; j < row_array.size(); j++) { // for all elements in the row
							if (row_array.isNull(j)) {
								elements.add(null);
							} else if (row_array.get(j).getValueType().equals(JsonValue.ValueType.STRING)) {
								elements.add(row_array.getString(j));
							} else if (row_array.get(j).getValueType().equals(JsonValue.ValueType.NUMBER)) {
								elements.add(row_array.getInt(j));
							} else if (row_array.get(j).getValueType().equals(JsonValue.ValueType.FALSE)
									|| row_array.get(j).getValueType().equals(JsonValue.ValueType.TRUE)) {
								elements.add(row_array.getBoolean(j));
							}

						}
						table.put(elements); // put the array of elements (the row) into the table
					}
					db.create(table);
					return table;
				} else {
					return "File does not exit";
				}

			} else { //xml mode
				try {
					if (path.toFile().exists()) {

						Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(path.toFile());

						Element root = doc.getDocumentElement();
						root.normalize();

						// SCHEMA
						//
						String table_name;
						if (tableName == null) { // if table name not defined in query
							table_name = root.getAttribute("name"); // set table name to that found in file
						} else { // else
							table_name = tableName; // set table name to that defined in query
						}

						int tableCounter = 1;
						while (db.exists(table_name)) { // while table name exists
							if (tableCounter == 1) { // if first time finding same table
								table_name = table_name + "_" + tableCounter; // add underscore and number to represent number of times found
							} else { // have already made a table name same as one considered in if case
								table_name = table_name.substring(0, table_name.length() - 1) + tableCounter; // replace number at end to represent how many times found
							}
							tableCounter++;
						}

						List<String> column_names = new LinkedList<>();
						List<FieldType> column_types = new LinkedList<>();

						Element columns_elem = (Element) root.getElementsByTagName("columns").item(0);

						NodeList column_nodes = columns_elem.getElementsByTagName("column");
						for (int i = 0; i < column_nodes.getLength(); i++) {
							Element column_elem = (Element) column_nodes.item(i);
							column_names.add(column_elem.getAttribute("name"));
							column_types.add(FieldType.valueOf(column_elem.getAttribute("type")));
						}

						int primary_index = Integer.parseInt(columns_elem.getAttribute("primary"));

						Table table = new HashArrayTable(table_name, column_names, column_types, primary_index);

						// STATE
						//
						NodeList row_nodes = root.getElementsByTagName("row");
						Element row_elem;
						NodeList field_nodes;

						for (int i = 0; i < row_nodes.getLength(); i++) {
							row_elem = (Element) row_nodes.item(i);
							field_nodes = row_elem.getElementsByTagName("field");
							ArrayList<Object> elements = new ArrayList<Object>(); // create an array to hold all elements in this row
							for (int j = 0; j < field_nodes.getLength(); j++) {
								if (((Element) field_nodes.item(j)).hasAttribute("null")) {
									elements.add(null);
								} else if (column_types.get(j) == FieldType.STRING) {
									elements.add(field_nodes.item(j).getTextContent()); // add element to elements array
								} else if (column_types.get(j) == FieldType.INTEGER) {
									elements.add(Integer.parseInt(field_nodes.item(j).getTextContent())); // add element to elements array
								} else if (column_types.get(j) == FieldType.BOOLEAN) {
									elements.add(Boolean.parseBoolean(field_nodes.item(j).getTextContent())); // add element to elements array
								}
							}
							table.put(elements);
						}
						db.create(table);
						return table;
					} else {
						return "File not found";
					}
				} catch (IOException | ParserConfigurationException | SAXException e) {
					throw new RuntimeException(e);
				}
			}

		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}
}
