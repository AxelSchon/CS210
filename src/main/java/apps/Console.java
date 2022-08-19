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
		try (
			final Database db = new Database(true);
			final Scanner in = new Scanner(System.in);
			final PrintStream out = System.out;
		) {
			out.print(">> ");

			String query = in.nextLine();
			try {
				out.println("Result: " + db.interpret(query));
			}
			catch (QueryError e) {
				out.println("Error: " + e);
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
