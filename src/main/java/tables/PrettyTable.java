package tables;

import java.util.ArrayList;

/**
 * Defines the protocols for a table
 * with a pretty string representation.
 * <p>
 * Do not modify existing protocols,
 * but you may add new protocols.
 */
public abstract class PrettyTable extends Table {
	@SuppressWarnings("unchecked")
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		var rows = new ArrayList<>(rows()); // list of elements from collection

		// sort rows
		rows.sort((row1, row2) -> {
			var key1 = (Comparable<Object>) row1.get(getPrimaryIndex());
			var key2 = (Comparable<Object>) row2.get(getPrimaryIndex());
			return key1.compareTo(key2);
		});
		sb.append("\n");
		sb.append("  --------------------").append("\n"); // lid of table name container
		sb.append(" /"); // first slash in row
		sb.append(String.format(" %-18s ", getTableName())); // table name
		sb.append("\\").append("\n"); // last slash in row

		// start column headers container
		sb.append("+"); // first corner in row separator
		for (int i = 0; i < getColumnTypes().size(); i++) {
			sb.append("----------------------+");
		}
		sb.append("\n");

		// print column headers
		for (int i = 0; i < getColumnNames().size(); i++) {
			if (i == getPrimaryIndex()) {
				if (getColumnTypes().get(i).getTypeNumber() == 2) {
					sb.append("|"); // first pipe in row
					sb.append(String.format(" %20s ", getColumnNames().get(i) + "*"));
				} else {
					sb.append("|"); // first pipe in row
					sb.append(String.format(" %-20s ", getColumnNames().get(i) + "*"));
				}
			} else if (getColumnTypes().get(i).getTypeNumber() == 2) {
				sb.append("|"); // first pipe in row
				sb.append(String.format(" %20s ", getColumnNames().get(i)));
			} else {
				sb.append("|"); // first pipe in row
				sb.append(String.format(" %-20s ", getColumnNames().get(i)));
			}
		}
		// end column headers container
		sb.append("|");
		sb.append("\n");
		sb.append("+"); // first corner in row separator
		for (int i = 0; i < getColumnTypes().size(); i++) {
			sb.append("======================+");
		}
		sb.append("\n");

		// print rows
		int cellSize = 18; // define size of cells
		for (var row : rows) {
			for (var element : row) {
				if (element instanceof String) {
					if (((String) element).length() > cellSize) { // if string is too wide for its column
						sb.append("|"); // first pipe in row
						sb.append(String.format(" \"%-20s", ((String) element).substring(0, 15) + "..."));
					} else {
						sb.append("|"); // first pipe in row
						sb.append(String.format(" \"%-19s ", element + "\"")); // format element into cell with size 20 char. (left aligned)
					}
				} else if (element instanceof Integer) {
					sb.append("|"); // first pipe in row
					sb.append(String.format(" %20s ", element)); // format element into cell with size 20 char (right aligned)
				} else if (element == null) {
					sb.append("|"); // first pipe in row
					sb.append(String.format(" %-20s ", "")); // format element into cell with size 20 char (left aligned)
				} else {
					sb.append("|"); // first pipe in row
					sb.append(String.format(" %-20s ", element)); // format element into cell with size 20 char (left aligned)
				}
			}
			sb.append("|");
			sb.append("\n");
			sb.append("+");
			for (int i = 0; i < getColumnTypes().size(); i++) {
				sb.append("----------------------+");
			}
			sb.append("\n");
		}
		return sb.toString();
	}

}
