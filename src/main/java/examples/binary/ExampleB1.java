package examples.binary;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

/**
 * Demonstrates binary de/serialization
 * for the metadata of a table.
 */
public class ExampleB1 {
	public static void main(String[] args) {
		// DUMMY FIELDS
		Path root = Paths.get("data", "tables", "example_b1");
		FileChannel metadata;

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
			metadata = FileChannel.open(root.resolve("metadata"), CREATE_NEW, READ, WRITE);

			// WRITE METADATA
			{
				var buf = metadata.map(READ_WRITE, 0, 4 * 2);

				buf.putInt(8); // DUMMY SIZE, WIDTH 4
				buf.putInt(-2106826295); // DUMMY FINGERPRINT, WIDTH 4
			}

			// REOPEN EXISTING TREE
			if (Files.notExists(root.resolve("metadata")))
				throw new IOException("Missing metadata file");
			metadata = FileChannel.open(root.resolve("metadata"), READ);

			// READ METADATA
			{
				var buf = metadata.map(READ_ONLY, 0, 4 * 2);

				System.out.println(buf.getInt()); // DUMMY SIZE, WIDTH 4
				System.out.println(buf.getInt()); // DUMMY FINGERPRINT, WIDTH 4
			}
		}
		catch (IOException e) {
			throw new RuntimeException("Metadata file I/O error", e);
		}
	}
}
