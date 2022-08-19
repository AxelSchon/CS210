package drivers;

import apps.Database;
import sql.Driver;
import sql.QueryError;

/*
 * TIMES TABLE 4
 *   -> result set:
 *     schema:
 * 	     x INTEGER PRIMARY
 *       x2 INTEGER
 *       x3 INTEGER
 *       x4 INTEGER
 *	   state:
 *       [1, 2,  3,  4]
 *       [2, 4,  6,  8]
 *       [3, 6,  9, 12]
 *       [4, 8, 12, 16]
 *
 * TIMES TABLE 4 AS num
 *   -> result set:
 *     schema:
 * 	     num INTEGER PRIMARY
 *       num_x2 INTEGER
 *       num_x3 INTEGER
 *       num_x4 INTEGER
 *     state:
 *       see TIMES TABLE 4
 *
 * TIMES TABLE 3 BY 5
 *   -> result set:
 *     schema:
 * 	     x PRIMARY INTEGER
 *       x2 INTEGER
 *       x3 INTEGER
 *       x4 INTEGER
 *       x5 INTEGER
 *	   state:
 *       [1, 2,  3,  4,  5]
 *       [2, 4,  6,  8, 10]
 *       [3, 6,  9, 12, 15]
 *
 * TIMES TABLE 3 BY 5 AS val
 *   -> result set:
 *     schema:
 * 	     val INTEGER PRIMARY
 *       val_x2 INTEGER
 *       val_x3 INTEGER
 *       val_x4 INTEGER
 *       val_x5 INTEGER
 *	   state:
 *       see TIMES TABLE 3 BY 5
 */
@Deprecated
public class TimesTable implements Driver {
	/*
	 * TODO: Implement stub for Practice 2 (optional)
	 * according to the documented examples above.
	 */

	@Override
	public boolean parse(String query) throws QueryError {
		return false;
	}

	@Override
	public Object execute(Database db) throws QueryError {
		return null;
	}
}
