package examples.xml;

import static sql.FieldType.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

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
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import sql.FieldType;
import tables.SearchTable;
import tables.Table;

/**
 * Demonstrates de/serialization in XML
 * for the schema of a table, assuming:
 * <p>
 * The table name is already known.
 * <p>
 * The properties are hard-coded
 * when exporting only.
 */
public class ExampleX1 {
	public static void main(String[] args) {
		Path path = Paths.get("data", "exports", "example_x1.xml");

		write(path);

		Table table = read(path);
		System.out.println(table);
	}

	// Using DOM (Document Object Model)

	public static void write(Path path) {
		try {
			Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();

			Element root = doc.createElement("table");
			root.setAttribute("name", "example_x1");
			doc.appendChild(root);

				Element columns = doc.createElement("columns");
				columns.setAttribute("primary", "0");
				root.appendChild(columns);

					Element column = doc.createElement("column");
					column.setAttribute("name", "letter");
					column.setAttribute("type", STRING.toString());
					columns.appendChild(column);

					column = doc.createElement("column");
					column.setAttribute("name", "order");
					column.setAttribute("type", INTEGER.toString());
					columns.appendChild(column);

					column = doc.createElement("column");
					column.setAttribute("name", "vowel");
					column.setAttribute("type", BOOLEAN.toString());
					columns.appendChild(column);

			Files.createDirectories(path.getParent());
		    Source from = new DOMSource(doc);
		    Result to = new StreamResult(path.toFile());
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
		    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		    transformer.transform(from, to);
		}
		catch (IOException | ParserConfigurationException | TransformerException e) {
			throw new RuntimeException(e);
		}
	}

	public static Table read(Path path) {
		try {
			Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(path.toFile());

			Element root = doc.getDocumentElement();
			root.normalize();

			String table_name = root.getAttribute("name");

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

			Table table = new SearchTable(
				table_name,
				column_names,
				column_types,
				primary_index
			);

			return table;
		}
		catch (IOException | ParserConfigurationException | SAXException e) {
			throw new RuntimeException(e);
		}
	}
}
