package grade;

import static sql.FieldType.INTEGER;

import org.junit.jupiter.api.BeforeAll;

import sql.QueryError;
import tables.Table;

@Deprecated
public class Practice2 extends AbstractQueryContainer {
	@BeforeAll
	void defineQueries() {
		queryData = new Object[][]{
			// SQUARE
			{ Table.class, "TIMES TABLE 4", null },
			{ Table.class, "TIMES TABLE 3", null },
			{ Table.class, "TIMES TABLE 5", null },
			{ Table.class, "TIMES TABLE 12", null },
			{ Table.class, "TIMES TABLE 1", "dimensions down to 1 allowed" },
			{ Table.class, "TIMES TABLE 15", "dimensions up to 15 allowed" },
			{ QueryError.class, "TIMES TABLE 0", "dimensions under 1 forbidden" },
			{ QueryError.class, "TIMES TABLE 16", "dimensions over 15 forbidden" },

			// SQUARE WITH ALIASING
			{ Table.class, "TIMES TABLE 4 AS num", "aliasing allowed" },
			{ Table.class, "TIMES TABLE 4 AS val", "aliasing allowed" },
			{ Table.class, "TIMES TABLE 4 AS y", "aliasing allowed" },
			{ Table.class, "TIMES TABLE 4 AS factor", "aliasing allowed" },
			{ Table.class, "TIMES TABLE 4 AS x", "alias x differs from default x" },

			// RECTANGULAR
			{ Table.class, "TIMES TABLE 3 BY 5", null },
			{ Table.class, "TIMES TABLE 1 BY 12", "rows down to 1 allowed" },
			{ Table.class, "TIMES TABLE 15 BY 7", "rows up to 15 allowed" },
			{ Table.class, "TIMES TABLE 7 BY 1", "columns down to 1 allowed" },
			{ Table.class, "TIMES TABLE 12 BY 15", "columns up to 15 allowed" },
			{ QueryError.class, "TIMES TABLE 0 BY 10", "rows under 1 forbidden" },
			{ QueryError.class, "TIMES TABLE 16 BY 6", "rows over 15 forbidden" },
			{ QueryError.class, "TIMES TABLE 6 BY 0", "columns under 1 forbidden" },
			{ QueryError.class, "TIMES TABLE 10 BY 16", "columns over 15 forbidden" },

			// RECTANGULAR WITH ALIASING
			{ Table.class, "TIMES TABLE 3 BY 5 AS i", "aliasing allowed for two dimensions" },
			{ Table.class, "TIMES TABLE 3 BY 5 AS x", "alias x differs from default x" },

			// SYNTAX
			{ Table.class, "TIMES TABLE 3 AS abcdefghijk", "aliases up to 11 characters allowed" },
			{ Table.class, "times table 4", "lower case keywords allowed" },
			{ Table.class, "TiMeS tAbLe 4", "mixed case keywords allowed" },
			{ Table.class, " TIMES TABLE 4 ", "unstripped whitespace allowed" },
			{ Table.class, "TIMES  TABLE \t 4", "excess internal whitespace allowed" },

			{ QueryError.class, "TIMES TABLE 4 AS abcdefghijkl", "aliases over 11 characters forbidden" },
			{ QueryError.class, "TIMES TABLE 4 AS", "aliases under 1 character forbidden" },
			{ QueryError.class, "TIMESTABLE4", "whitespace between keywords and data required" },
			{ QueryError.class, "TIMES 4", "TABLE keyword required" },
			{ QueryError.class, "TABLE 4", "TIMES keyword required" },
			{ QueryError.class, "TIMES BELOW", "integer literal required" },
			{ QueryError.class, "TIMES BELOW 4 num", "AS keyword required for aliasing" },
			{ QueryError.class, "TIMES BELOW 3 5", "BY keyword required for two dimensions" },
			{ QueryError.class, "TIMES BELOW 3 BY 5 num", "AS keyword required for aliasing with two dimensions" },

			// ROBUSTNESS
			{ String.class, "ECHO \"Hello, world!\"", null },
			{ QueryError.class, "AN UNRECOGNIZABLE QUERY", null }
		};

		controlData = new Object[][]{
			{ "_times", 4, 0, "x", "x2", "x3", "x4", INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 2, 4, 6, 8, 3, 6, 9, 12, 4, 8, 12, 16 },
			{ "_times", 3, 0, "x", "x2", "x3", INTEGER, INTEGER, INTEGER, 1, 2, 3, 2, 4, 6, 3, 6, 9 },
			{ "_times", 5, 0, "x", "x2", "x3", "x4", "x5", INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 5, 2, 4, 6, 8, 10, 3, 6, 9, 12, 15, 4, 8, 12, 16, 20, 5, 10, 15, 20, 25 },
			{ "_times", 12, 0, "x", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9", "x10", "x11", "x12", INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72, 7, 14, 21, 28, 35, 42, 49, 56, 63, 70, 77, 84, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 96, 9, 18, 27, 36, 45, 54, 63, 72, 81, 90, 99, 108, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 11, 22, 33, 44, 55, 66, 77, 88, 99, 110, 121, 132, 12, 24, 36, 48, 60, 72, 84, 96, 108, 120, 132, 144 },
			{ "_times", 1, 0, "x", INTEGER, 1 },
			{ "_times", 15, 0, "x", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9", "x10", "x11", "x12", "x13", "x14", "x15", INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72, 78, 84, 90, 7, 14, 21, 28, 35, 42, 49, 56, 63, 70, 77, 84, 91, 98, 105, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 96, 104, 112, 120, 9, 18, 27, 36, 45, 54, 63, 72, 81, 90, 99, 108, 117, 126, 135, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 11, 22, 33, 44, 55, 66, 77, 88, 99, 110, 121, 132, 143, 154, 165, 12, 24, 36, 48, 60, 72, 84, 96, 108, 120, 132, 144, 156, 168, 180, 13, 26, 39, 52, 65, 78, 91, 104, 117, 130, 143, 156, 169, 182, 195, 14, 28, 42, 56, 70, 84, 98, 112, 126, 140, 154, 168, 182, 196, 210, 15, 30, 45, 60, 75, 90, 105, 120, 135, 150, 165, 180, 195, 210, 225 },
			null,
			null,
			{ "_times", 4, 0, "num", "num_x2", "num_x3", "num_x4", INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 2, 4, 6, 8, 3, 6, 9, 12, 4, 8, 12, 16 },
			{ "_times", 4, 0, "val", "val_x2", "val_x3", "val_x4", INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 2, 4, 6, 8, 3, 6, 9, 12, 4, 8, 12, 16 },
			{ "_times", 4, 0, "y", "y_x2", "y_x3", "y_x4", INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 2, 4, 6, 8, 3, 6, 9, 12, 4, 8, 12, 16 },
			{ "_times", 4, 0, "factor", "factor_x2", "factor_x3", "factor_x4", INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 2, 4, 6, 8, 3, 6, 9, 12, 4, 8, 12, 16 },
			{ "_times", 4, 0, "x", "x_x2", "x_x3", "x_x4", INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 2, 4, 6, 8, 3, 6, 9, 12, 4, 8, 12, 16 },
			{ "_times", 5, 0, "x", "x2", "x3", "x4", "x5", INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 5, 2, 4, 6, 8, 10, 3, 6, 9, 12, 15 },
			{ "_times", 12, 0, "x", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9", "x10", "x11", "x12", INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 },
			{ "_times", 7, 0, "x", "x2", "x3", "x4", "x5", "x6", "x7", INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 5, 6, 7, 2, 4, 6, 8, 10, 12, 14, 3, 6, 9, 12, 15, 18, 21, 4, 8, 12, 16, 20, 24, 28, 5, 10, 15, 20, 25, 30, 35, 6, 12, 18, 24, 30, 36, 42, 7, 14, 21, 28, 35, 42, 49, 8, 16, 24, 32, 40, 48, 56, 9, 18, 27, 36, 45, 54, 63, 10, 20, 30, 40, 50, 60, 70, 11, 22, 33, 44, 55, 66, 77, 12, 24, 36, 48, 60, 72, 84, 13, 26, 39, 52, 65, 78, 91, 14, 28, 42, 56, 70, 84, 98, 15, 30, 45, 60, 75, 90, 105 },
			{ "_times", 1, 0, "x", INTEGER, 1, 2, 3, 4, 5, 6, 7 },
			{ "_times", 15, 0, "x", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9", "x10", "x11", "x12", "x13", "x14", "x15", INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72, 78, 84, 90, 7, 14, 21, 28, 35, 42, 49, 56, 63, 70, 77, 84, 91, 98, 105, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 96, 104, 112, 120, 9, 18, 27, 36, 45, 54, 63, 72, 81, 90, 99, 108, 117, 126, 135, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 11, 22, 33, 44, 55, 66, 77, 88, 99, 110, 121, 132, 143, 154, 165, 12, 24, 36, 48, 60, 72, 84, 96, 108, 120, 132, 144, 156, 168, 180 },
			null,
			null,
			null,
			null,
			{ "_times", 5, 0, "i", "i_x2", "i_x3", "i_x4", "i_x5", INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 5, 2, 4, 6, 8, 10, 3, 6, 9, 12, 15 },
			{ "_times", 5, 0, "x", "x_x2", "x_x3", "x_x4", "x_x5", INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 5, 2, 4, 6, 8, 10, 3, 6, 9, 12, 15 },
			{ "_times", 3, 0, "abcdefghijk", "abcdefghijk_x2", "abcdefghijk_x3", INTEGER, INTEGER, INTEGER, 1, 2, 3, 2, 4, 6, 3, 6, 9 },
			{ "_times", 4, 0, "x", "x2", "x3", "x4", INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 2, 4, 6, 8, 3, 6, 9, 12, 4, 8, 12, 16 },
			{ "_times", 4, 0, "x", "x2", "x3", "x4", INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 2, 4, 6, 8, 3, 6, 9, 12, 4, 8, 12, 16 },
			{ "_times", 4, 0, "x", "x2", "x3", "x4", INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 2, 4, 6, 8, 3, 6, 9, 12, 4, 8, 12, 16 },
			{ "_times", 4, 0, "x", "x2", "x3", "x4", INTEGER, INTEGER, INTEGER, INTEGER, 1, 2, 3, 4, 2, 4, 6, 8, 3, 6, 9, 12, 4, 8, 12, 16 },
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			null
		};
	}
}