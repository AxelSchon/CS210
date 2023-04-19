package drivers;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import apps.Database;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonWriter;
import jakarta.json.JsonWriterFactory;
import jakarta.json.stream.JsonGenerator;
import sql.Driver;
import sql.FieldType;
import sql.QueryError;

/*
 * SHOW TABLE example_table
 * 	 -> table: example_table from database
 */
public class Export implements Driver {
	// Export\s+([a-z][a-z0-9_]*)\s+(?:To\s+([a-z][a-z0-9_]*).(xml|json)|AS\s+(xml|json))
	private static final Pattern pattern = Pattern.compile(
			"Export\\s+(?<tableName>[a-z][a-z0-9_]*)\\s+(?:To\\s+(?<fileName>[a-z][a-z0-9_]*).(xml|json)|AS\\s+(xml|json))",
			Pattern.CASE_INSENSITIVE);

	// define fields
	String tableName = null;
	String fileName = null;
	String fileType = null;
	boolean jsonMode = false;

	@Override
	public boolean parse(String query) throws QueryError {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		// initialize fields
		tableName = matcher.group("tableName"); // get the table name

		if (matcher.group(3) != null) { // 'To file_name' form
			fileName = matcher.group("fileName");// get the file name
			fileType = matcher.group(3);
		} else {
			fileType = matcher.group(4); // 'As' form
			fileName = tableName;
		}

		if (fileType.equalsIgnoreCase("json")) {
			jsonMode = true;
		}

		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {

		// if the table doesn't exist in the database:
		if (!db.exists(tableName)) {
			throw new QueryError("Table not found"); // throw an error
		}

		// define path to store file
		int fileCounter = 1;
		while (Files.exists(Paths.get("data", "exports", fileName))) { // while file name exists
			if (fileCounter == 1) { // if first time finding same fileName
				fileName = fileName + "_" + fileCounter; // add underscore and number to represent number of times found
			} else { // have already made a file name same as one considered in if case
				fileName = fileName.substring(0, fileName.length() - 1) + fileCounter; // replace number at end to represent how many times found
			}
			fileCounter++;
		}

		// define sourceTable reference
		var sourceTable = db.find(tableName);
		var columnNames = sourceTable.getColumnNames();
		var columnTypes = sourceTable.getColumnTypes();
		var primaryIndex = sourceTable.getPrimaryIndex();

		// json mode
		if (jsonMode == true) {
			try {

				Path path = Paths.get("data", "exports", fileName + ".json");

				JsonObjectBuilder root_object_builder = Json.createObjectBuilder();

				//
				//   SCHEMA 
				//
				JsonObjectBuilder schema_builder = Json.createObjectBuilder();

				schema_builder.add("table_name", tableName);

				// Column Names Builder
				JsonArrayBuilder column_names_builder = Json.createArrayBuilder();

				for (int i = 0; i < columnNames.size(); i++) { // for each column name
					column_names_builder.add(columnNames.get(i)); // add that column name to the column names builder
				}

				schema_builder.add("column_names", column_names_builder.build()); // add column names builder to the root

				// Column Types Builder
				JsonArrayBuilder column_types_builder = Json.createArrayBuilder();

				for (int i = 0; i < columnTypes.size(); i++) { // for each column type
					column_types_builder.add(columnTypes.get(i).toString()); // add that column type to the column types builder
				}

				schema_builder.add("column_types", column_types_builder.build()); // add column types builder to the root

				// Primary Index
				schema_builder.add("primary_index", primaryIndex); // add primary index to the root

				root_object_builder.add("schema", schema_builder.build());

				//
				//  State 
				//

				JsonArrayBuilder root_array_builder = Json.createArrayBuilder();

				JsonArrayBuilder row_builder = Json.createArrayBuilder();

				for (var sourceRow : sourceTable) { // for every row in the table
					for (int i = 0; i < sourceRow.size(); i++) { // for every element in that row
						if (sourceRow.get(i) == null) {
							row_builder.addNull();
						} else if (columnTypes.get(i) == FieldType.STRING) { // if element is a string
							row_builder.add((String) sourceRow.get(i)); // cast element to string and add to the row_builder
						} else if (columnTypes.get(i) == FieldType.INTEGER) { // if element is an integer
							row_builder.add((int) sourceRow.get(i)); // cast element to int and add to the row_builder
						} else if (columnTypes.get(i) == FieldType.BOOLEAN) { // if element is a boolean
							row_builder.add((boolean) sourceRow.get(i)); // cast element to boolean and add to the row_builder
						}
					}
					root_array_builder.add(row_builder.build()); // add that row to the root builder
				}

				root_object_builder.add("state", root_array_builder.build());

				JsonObject root_object = root_object_builder.build();

				Files.createDirectories(path.getParent());
				JsonWriterFactory factory = Json.createWriterFactory(Map.of(JsonGenerator.PRETTY_PRINTING, true));
				JsonWriter writer = factory.createWriter(new FileOutputStream(path.toFile()));
				writer.writeObject(root_object);
				writer.close();

			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		//xml mode
		else {
			try {

				Path path = Paths.get("data", "exports", fileName + ".xml");
				Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();

				//
				// SCHEMA
				//

				// create root
				Element root = doc.createElement("table");
				root.setAttribute("name", tableName);
				doc.appendChild(root);

				Element schema = doc.createElement("schema");
				root.appendChild(schema);

				Element columns = doc.createElement("columns");
				columns.setAttribute("primary", String.valueOf(primaryIndex)); // set primary Index
				schema.appendChild(columns);

				Element column = doc.createElement("column");
				for (int i = 0; i < columnNames.size(); i++) { // for each column
					column = doc.createElement("column");
					column.setAttribute("name", columnNames.get(i)); // set name attribute
					column.setAttribute("type", columnTypes.get(i).toString()); // set type attribute
					columns.appendChild(column); // append that column to columns
				}

				//
				// STATE
				//

				Element state = doc.createElement("state");
				root.appendChild(state);

				Element row = doc.createElement("row");

				Element field = doc.createElement("field");

				for (var sourceRow : sourceTable) { // for every row in the table
					row = doc.createElement("row"); // create new row
					for (int i = 0; i < sourceRow.size(); i++) { // for every element in that row
						if (sourceRow.get(i) == null) {
							field = doc.createElement("field");
							field.setAttribute("null", "yes");
							row.appendChild(field);
						} else {
							field = doc.createElement("field"); // create new field
							field.setTextContent(String.valueOf(sourceRow.get(i))); // assign content of field
							row.appendChild(field); // append field to row
						}
					}
					state.appendChild(row); // append row to root
				}

				Files.createDirectories(path.getParent());
				Source from = new DOMSource(doc);
				Result to = new StreamResult(path.toFile());
				Transformer transformer = TransformerFactory.newInstance().newTransformer();
				transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
				transformer.setOutputProperty(OutputKeys.INDENT, "yes");
				transformer.transform(from, to);
			} catch (IOException | ParserConfigurationException | TransformerException e) {
				throw new RuntimeException(e);
			}
		}

		return true;
	}
}
