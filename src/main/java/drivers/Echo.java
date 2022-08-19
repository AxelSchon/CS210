package drivers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import sql.Driver;

/*
 * ECHO "Hello, world!"
 * 	 -> string: Hello, world!
 */
public class Echo implements Driver {
	private static final Pattern pattern = Pattern.compile(
		"ECHO\\s*\"([^\"]*)\"",
		Pattern.CASE_INSENSITIVE
	);

	private String text;

	@Override
	public boolean parse(String query) {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		text = matcher.group(1);

		return true;
	}

	@Override
	public Object execute(Database db) {
		return text;
	}
}
