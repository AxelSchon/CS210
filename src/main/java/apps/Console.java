package apps;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Scanner;

import sql.QueryError;
import tables.Table;

/**
 * Implements a user console for
 * interacting with a database.
 * <p>
 * Do not modify existing protocols,
 * but you may add new protocols.
 */
public class Console {
	/*
	 * TODO: Implement stub for Module 3.
	 */

	/**
	 * The entry point for execution
	 * with user input/output.
	 */
	public static void main(String[] args) {
		try (final Database db = new Database(true); // true: database persistent in storage (dependent on M2). false: non-persistent 
				final Scanner in = new Scanner(System.in); // keyboard input
				final PrintStream out = System.out;) { // destination. system.out is console output

			while (true) {
				out.print(">> "); // prints out prompt.(>>)

				String script = in.nextLine().strip(); // get query input from user

				if (script.toUpperCase().equals("EXIT")) // if sentinel is entered, exit REPL
					break;

				String[] queries = script.split(";"); // split user input on semicolon
				for (String query : queries) { // for every token:
					query = query.strip(); // remove white space from query

					if (query.startsWith("--")) //// if input is a comment, skip to next run of REPL
						continue;

					if (query.isBlank())// if the query is blank:
						continue; // skip to the next run of the loop

					out.println("Query: " + query); // print the query back

					try {
						var res = db.interpret(query); // determines what this instruction is supposed to do on data this database has and return meaningful result

						// use instance of to check the type and branch accordingly
						if (res instanceof Table) { // distinguish table from result set by checking if the name begins with an underscore or not (underscore means result set)
							if ((((Table) res).getTableName().startsWith("_")))
								out.println("Result Set: " + res.toString());
							else
								out.println("Table: " + res.toString());
						}
						if (res instanceof Integer)
							out.println("Number of affected rows: " + res);
						if (res instanceof String || res instanceof Boolean)
							out.println("Result: " + res);
					} catch (QueryError e) {
						out.println("Error: " + e);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
