package examples.binary;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.*;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

/**
 * Demonstrates binary de/serialization
 * for the state of a table.
 */
public class ExampleB3 {
	public static void main(String[] args) {
		// DUMMY FIELDS
		Path root = Paths.get("data", "tables", "example_b3");

		try {
			// DELETE EXISTING TREE
			if (Files.exists(root)) {
				Files.walk(root)
					.sorted(Comparator.reverseOrder())
					.map(path -> path.toFile())
					.forEach(file -> file.delete());
			}

			// DEFINE PATH FROM DIGEST
			var path = root.resolve("state").resolve("a1").resolve("b2c3");

			// CREATE NEW TREE
			Files.createDirectories(path.getParent());

			// WRITE STATE
			{
				var channel = FileChannel.open(path, CREATE, READ, WRITE);
				var buf = channel.map(READ_WRITE, 0, 1+127 + 1+4 + 1);

				// DUMMY FIELD 0 STRING
				// WIDTH: 1 + max string length
				{
					var str = "alpha"; // DUMMY LETTER VALUE
					var chars = str.getBytes(UTF_8);
					buf.put((byte) chars.length);
					buf.put(chars);
				}

				// DUMMY FIELD 1 INTEGER-AS-SHORT
				// WIDTH: 1 + 2
				{
					buf.put((byte) Short.BYTES);
					buf.putShort((short) 1); // DUMMY ORDER VALUE
				}

				// DUMMY FIELD 2 BOOLEAN
				// WIDTH: 1
				{
					buf.put((byte) 1); // DUMMY VOWEL VALUE
				}
			}

			// REOPEN EXISTING TREE
			if (Files.notExists(path))
				throw new IOException("Missing state file");

			// READ STATE
			{
				var channel = FileChannel.open(path, READ);
				var buf = channel.map(READ_ONLY, 0, 1+127 + 1+4 + 1);

				// DUMMY FIELD 0 STRING
				// WIDTH: 1 + max string length
				{
					var len = buf.get();
					var chars = new byte[len];
					buf.get(chars);
					System.out.println(new String(chars, UTF_8)); // DUMMY LETTER VALUE
				}

				// DUMMY FIELD 1 INTEGER-AS-SHORT
				// WIDTH: 1 + 2
				{
					buf.get();
					System.out.println(buf.getShort()); // DUMMY ORDER VALUE
				}

				// DUMMY FIELD 2 BOOLEAN
				// WIDTH: 1
				{
					System.out.println(buf.get() == 1); // DUMMY VOWEL VALUE
				}
			}
		}
		catch (IOException e) {
			throw new RuntimeException("State file I/O error", e);
		}
	}
}
