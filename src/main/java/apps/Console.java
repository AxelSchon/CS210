package apps;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Scanner;

import sql.QueryError;

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
		try (final Database db = new Database(true);
				final Scanner in = new Scanner(System.in);
				final PrintStream out = System.out;) {

			{// TODO: make this a loop which stops on input EXIT (case insensitive) (not script, not query)
				out.print(">> ");

				// 
				String script = in.nextLine().strip();

				// if input is a comment, skip to next run of REPL

				var queries = script.split(";");

				for (String query : queries) { // must have check for EXIT somewhere inside loop?
					query = query.strip();
					// if the query is blank, skip to the next run of the loop ( String should have method for this?)

					// print the query back
					out.println("Query: " + query); // my code (might be wrong)
					try {
						var res = db.interpret(query);
						// use instance of to check the type
						// branch accordingly
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
