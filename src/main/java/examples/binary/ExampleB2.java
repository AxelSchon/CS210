package examples.binary;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.*;
import static sql.FieldType.*;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import sql.FieldType;

/**
 * Demonstrates binary de/serialization
 * for the schema of a table.
 */
public class ExampleB2 {
	public static void main(String[] args) {
		// DUMMY FIELDS
		Path root = Paths.get("data", "tables", "example_b2");
		FileChannel schema;

		try {
			// DELETE EXISTING TREE
			if (Files.exists(root)) {
				Files.walk(root)
					.sorted(Comparator.reverseOrder())
					.map(path -> path.toFile())
					.forEach(file -> file.delete());
			}

			// CREATE NEW TREE
			Files.createDirectories(root);
			schema = FileChannel.open(root.resolve("schema"), CREATE_NEW, READ, WRITE);

			// WRITE SCHEMA
			{
				var buf = schema.map(READ_WRITE, 0, 4*2 + (1+15+1)*15);

				buf.putInt(3); // DUMMY COLUMN COUNT, WIDTH 4
				buf.putInt(0); // DUMMY PRIMARY INDEX, WIDTH 4

				// DUMMY COLUMN 0: letter STRING PRIMARY
				// WIDTH: 1 + maximum name length + 1
				{
					var name = "letter";
					var chars = name.getBytes(UTF_8);
					buf.put((byte) chars.length);
					buf.put(chars);
					buf.put(new byte[15 - chars.length]);

					var type = STRING;
					buf.put((byte) type.getTypeNumber());
				}

				// DUMMY COLUMN 1: order INTEGER
				// WIDTH: 1 + maximum name length + 1
				{
					var name = "order";
					var chars = name.getBytes(UTF_8);
					buf.put((byte) chars.length);
					buf.put(chars);
					buf.put(new byte[15 - chars.length]);

					var type = INTEGER;
					buf.put((byte) type.getTypeNumber());
				}

				// DUMMY COLUMN 2: vowel BOOLEAN
				// WIDTH: 1 + maximum name length + 1
				{
					var name = "vowel";
					var chars = name.getBytes(UTF_8);
					buf.put((byte) chars.length);
					buf.put(chars);
					buf.put(new byte[15 - chars.length]);

					var type = BOOLEAN;
					buf.put((byte) type.getTypeNumber());
				}
			}

			// REOPEN EXISTING TREE
			if (Files.notExists(root.resolve("schema")))
				throw new IOException("Missing schema file");
			schema = FileChannel.open(root.resolve("schema"), READ, WRITE);

			// READ SCHEMA
			{
				var buf = schema.map(READ_WRITE, 0, 4*2 + (1+15+1)*15);

				System.out.println(buf.getInt()); // DUMMY COLUMN COUNT, WIDTH 4
				System.out.println(buf.getInt()); // DUMMY PRIMARY INDEX, WIDTH 4

				// DUMMY COLUMN 0: letter STRING PRIMARY
				// WIDTH: 1 + maximum name length + 1
				{
					var length = buf.get();
					var chars = new byte[length];
					buf.get(chars);
					System.out.println(new String(chars, UTF_8));
					buf.get(new byte[15 - chars.length]);

					System.out.println(FieldType.valueOf(buf.get()));
				}

				// DUMMY COLUMN 1: order INTEGER
				// WIDTH: 1 + maximum name length + 1
				{
					var length = buf.get();
					var chars = new byte[length];
					buf.get(chars);
					System.out.println(new String(chars, UTF_8));
					buf.get(new byte[15 - chars.length]);

					System.out.println(FieldType.valueOf(buf.get()));
				}

				// DUMMY COLUMN 2: vowel BOOLEAN
				// WIDTH: 1 + maximum name length + 1
				{
					var length = buf.get();
					var chars = new byte[length];
					buf.get(chars);
					System.out.println(new String(chars, UTF_8));
					buf.get(new byte[15 - chars.length]);

					System.out.println(FieldType.valueOf(buf.get()));
				}
			}
		}
		catch (IOException e) {
			throw new RuntimeException("Schema file I/O error", e);
		}
	}
}
