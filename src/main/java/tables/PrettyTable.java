package tables;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Defines the protocols for a table
 * with a pretty string representation.
 * <p>
 * Do not modify existing protocols,
 * but you may add new protocols.
 */
public abstract class PrettyTable extends Table {
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		//set of list of rows from table (only has set operations)
		//var rows = rows();
		//instead, pass into ArrayList<>() so that sorting methods are available

		// -> is lambda expression (see lecture @ 9:28 10/5/22 to see more compact version of this)
		var rows = new ArrayList<>(rows());
		rows.sort(new Comparator<List<Object>>() {

			@SuppressWarnings("unchecked")
			@Override
			public int compare(List<Object> row1, List<Object> row2) {
				var key1 = (Comparable<Object>) row1.get(getPrimaryIndex());
				var key2 = (Comparable<Object>) row2.get(getPrimaryIndex());
				return key1.compareTo(key2);
			}
		});
		//TODO stringify header (table name and column names
		for (var row : rows) {
			sb.append(row).append("\n"); // TODO actually stringify the rows
		}
		return sb.toString();
	}
}
