package apps;

import static sql.FieldType.BOOLEAN;
import static sql.FieldType.INTEGER;
import static sql.FieldType.STRING;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import tables.SearchTable;
import tables.Table;

/**
 * Sandbox for execution of arbitrary code
 * for testing or grading purposes.
 * <p>
 * Modify the code for your use case.
 */
@Deprecated
public class Sandbox {
	//hexadecimal to a length of 40 chars
	public static String digest(Object key) {
		// consult the SHA-1 algorithm to produce a digest
		try {
			var sha1 = MessageDigest.getInstance("SHA-1");
			// "update" data into it as many times as necessary. Then "digest" built data. 
			sha1.update("verySaltySalt".getBytes(StandardCharsets.UTF_8));
			sha1.update(key.toString().getBytes(StandardCharsets.UTF_8));

			var digest = sha1.digest();
			var hex = HexFormat.of().withLowerCase();
			return hex.formatHex(digest);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("Digest error", e);
		}
	}

	public static void main(String[] args) {
		String key = "asdf";
		System.out.println(digest(key));

		// folder/file

		// folder: 00 (0) to FF (256) 
		//	 if a million folder, gets evenly distributed among 256 folders

		Table table = new SearchTable("sandbox_1", List.of("letter", "order", "vowel"),
				List.of(STRING, INTEGER, BOOLEAN), 0);

		table.put(List.of("alpha", 1, true));
		table.put(List.of("beta", 2, false));
		table.put(List.of("gamma", 3, false));
		table.put(List.of("delta", 4, false));
		table.put(List.of("tau", 19, false));
		table.put(List.of("pi", 16, false));
		table.put(List.of("omega", 24, true));
		table.put(Arrays.asList("N/A", null, null));

		System.out.println(table);
	}
}
