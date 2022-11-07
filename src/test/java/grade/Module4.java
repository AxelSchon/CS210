package grade;

import static sql.FieldType.*;

import org.junit.jupiter.api.BeforeAll;

import sql.QueryError;
import tables.Table;

public class Module4 extends AbstractQueryContainer {
	@BeforeAll
	void defineQueries() {
		queryData = new Object[][]{
			// BASICS
			{ Table.class, "CREATE TABLE m4_table01 (id INTEGER PRIMARY, name STRING, flag BOOLEAN)", null },
			{ Table.class, "SHOW TABLES", null },
			{ QueryError.class, "CREATE TABLE m4_table01 (id INTEGER PRIMARY, name STRING, flag BOOLEAN)", null },
			{ Table.class, "SHOW TABLES", null },

			// CASE, WHITESPACE
			{ Table.class, "create table M4_TABLE02 (ID integer primary, NAME string, flag BOOLEAN)", "lower case keyword and upper case table name allowed" },
			{ Table.class, " CREATE TABLE m4_table03 (id INTEGER PRIMARY, name STRING, flag BOOLEAN) ", "unstripped whitespace allowed" },
			{ Table.class, "CREATE  TABLE  m4_table04  (id INTEGER PRIMARY, name STRING, flag BOOLEAN)", "excess internal whitespace allowed" },
			{ Table.class, "CREATE TABLE m4_table05 ( id INTEGER PRIMARY , name STRING , flag BOOLEAN )", "excess internal whitespace allowed" },
			{ Table.class, "CREATE TABLE m4_table06 (id INTEGER PRIMARY,name STRING,flag BOOLEAN)", "whitespace around punctuation not required" },
			{ Table.class, "SHOW TABLES", null },
			{ QueryError.class, "CREATETABLE m4_table07 (id INTEGERPRIMARY, name STRING, flag BOOLEAN)", "whitespace between keywords required " },
			{ QueryError.class, "CREATE TABLEm4_table08 (idINTEGER PRIMARY, nameSTRING, flag BOOLEAN)", "whitespace between keywords and names required" },
			{ Table.class, "SHOW TABLES", null },

			// NAMES, KEYWORDS, PUNCTUATION
			{ Table.class, "CREATE TABLE t (i INTEGER PRIMARY, n STRING, f BOOLEAN)", "1-character name allowed" },
			{ Table.class, "CREATE TABLE m4_table10_____ (n23456789012345 INTEGER PRIMARY)", "15-character name allowed" },
			{ Table.class, "SHOW TABLES", null },
			{ QueryError.class, "CREATE TABLE m4_table11______ (n234567890123456 INTEGER PRIMARY)", "at most 15-character names allowed" },
			{ QueryError.class, "CREATE TABLE 1m_table12 (2id INTEGER PRIMARY, 3name STRING, 4flag BOOLEAN)", "name starting with number forbidden" },
			{ QueryError.class, "CREATE TABLE _m1table13 (_id INTEGER PRIMARY, _name STRING, _flag BOOLEAN)", "name starting with underscore forbidden" },
			{ QueryError.class, "CREATE TABLE (id INTEGER PRIMARY, name STRING, flag BOOLEAN)", "table name required" },
			{ QueryError.class, "CREATE m4_table15 (id INTEGER PRIMARY, name STRING, flag BOOLEAN)", "TABLE keyword required" },
			{ QueryError.class, "CREATE TABLE m4_table16 (id INTEGER PRIMARY name STRING flag BOOLEAN)", "commas between definitions required" },
			{ QueryError.class, "CREATE TABLE m4_table17 id INTEGER PRIMARY, name STRING, flag BOOLEAN", "parentheses required" },
			{ Table.class, "SHOW TABLES", null },

			// COLUMNS, PRIMARY
			{ Table.class, "CREATE TABLE m4_table18 (c1 INTEGER PRIMARY)", "1 column allowed" },
			{ Table.class, "CREATE TABLE m4_table19 (c1 INTEGER PRIMARY, c2 STRING)", "2 columns allowed" },
			{ Table.class, "CREATE TABLE m4_table20 (c1 INTEGER PRIMARY, c2 INTEGER, c3 INTEGER, c4 INTEGER, c5 INTEGER, c6 INTEGER, c7 INTEGER, c8 INTEGER, c9 INTEGER, c10 INTEGER, c11 INTEGER, c12 INTEGER, c13 INTEGER, c14 INTEGER, c15 INTEGER)", "15 columns allowed" },
			{ Table.class, "CREATE TABLE m4_table21 (id INTEGER, name STRING PRIMARY, flag BOOLEAN)", "non-0 primary column index allowed" },
			{ Table.class, "SHOW TABLES", null },
			{ QueryError.class, "CREATE TABLE m4_table22 (id INTEGER PRIMARY, other STRING, other BOOLEAN)", "duplicate column name forbidden" },
			{ QueryError.class, "CREATE TABLE m4_table23 (id INTEGER, name STRING, flag BOOLEAN)", "primary column required" },
			{ QueryError.class, "CREATE TABLE m4_table24 (id INTEGER PRIMARY, name STRING PRIMARY, flag BOOLEAN PRIMARY)", "duplicate primary column forbidden" },
			{ QueryError.class, "CREATE TABLE m4_table25 ()", "at least 1 column required" },
			{ QueryError.class, "CREATE TABLE m4_table26 (c1 INTEGER PRIMARY, c2 INTEGER, c3 INTEGER, c4 INTEGER, c5 INTEGER, c6 INTEGER, c7 INTEGER, c8 INTEGER, c9 INTEGER, c10 INTEGER, c11 INTEGER, c12 INTEGER, c13 INTEGER, c14 INTEGER, c15 INTEGER, c16 INTEGER)", "at most 15 columns allowed" },
			{ Table.class, "SHOW TABLES", null },

			// DROP TABLE
			{ Integer.class, "DROP TABLE m4_table01", null },
			{ QueryError.class, "DROP TABLE m4_table01", null },
			{ Table.class, "SHOW TABLES", null },
			{ Table.class, "CREATE TABLE m4_table01 (ps STRING PRIMARY)", null },
			{ Table.class, "SHOW TABLES", null },
			{ Integer.class, "drop table M4_TABLE02", "lower case keyword and upper case table name allowed" },
			{ Integer.class, " DROP TABLE m4_table03 ", "unstripped whitespace allowed" },
			{ Integer.class, "DROP  TABLE  m4_table04", "excess internal whitespace allowed" },
			{ Integer.class, "DROP TABLE t", "1-letter name allowed" },
			{ Table.class, "SHOW TABLES", null },
			{ QueryError.class, "DROPTABLE m4_table05", "whitespace between keywords required " },
			{ QueryError.class, "DROP TABLEm4_table06", "whitespace between keywords and names required" },
			{ QueryError.class, "DROP m4_table17", "TABLE keyword required" },
			{ QueryError.class, "DROP TABLE", "table name required" },
			{ Table.class, "SHOW TABLES", null },

			// SHOW TABLES
			{ Table.class, "SHOW TABLES", "upper case keyword allowed" },
			{ Table.class, "show tables", "lower case keyword allowed" },
			{ Table.class, "ShOw tAbLeS", "mixed case keyword allowed" },
			{ Table.class, "  SHOW  TABLES  ", "excess internal whitespace and unstripped whitespace allowed" },
			{ Table.class, "SHOW TABLES", null },
			{ QueryError.class, "SHOWTABLES", "whitespace between keywords required " },
			{ QueryError.class, "SHOW", "TABLES keyword required" },
			{ QueryError.class, "TABLES", "SHOW keyword required" },
			{ Table.class, "SHOW TABLES", null },

			// DATABASE INTERPRETER
			{ String.class, "ECHO \"Hello, world!\"", null },
			{ Table.class, "RANGE 5", null },
			{ Table.class, "SHOW TABLE m4_table01", null },
			{ QueryError.class, "SHOW TABLE m4_table00", null },
			{ QueryError.class, "AN UNRECOGNIZABLE QUERY", null },
			{ Table.class, "SHOW TABLES", null },
		};

		controlData = new Object[][]{
			{ "m4_table01", 3, 0, "id", "name", "flag", INTEGER, STRING, BOOLEAN },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "m4_table01", 3, 0 },
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "m4_table01", 3, 0 },
			{ "M4_TABLE02", 3, 0, "ID", "NAME", "flag", INTEGER, STRING, BOOLEAN },
			{ "m4_table03", 3, 0, "id", "name", "flag", INTEGER, STRING, BOOLEAN },
			{ "m4_table04", 3, 0, "id", "name", "flag", INTEGER, STRING, BOOLEAN },
			{ "m4_table05", 3, 0, "id", "name", "flag", INTEGER, STRING, BOOLEAN },
			{ "m4_table06", 3, 0, "id", "name", "flag", INTEGER, STRING, BOOLEAN },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "M4_TABLE02", 3, 0, "m4_table01", 3, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0 },
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "M4_TABLE02", 3, 0, "m4_table01", 3, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0 },
			{ "t", 3, 0, "i", "n", "f", INTEGER, STRING, BOOLEAN },
			{ "m4_table10_____", 1, 0, "n23456789012345", INTEGER },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "M4_TABLE02", 3, 0, "m4_table01", 3, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "t", 3, 0 },
			null,
			null,
			null,
			null,
			null,
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "M4_TABLE02", 3, 0, "m4_table01", 3, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "t", 3, 0 },
			{ "m4_table18", 1, 0, "c1", INTEGER },
			{ "m4_table19", 2, 0, "c1", "c2", INTEGER, STRING },
			{ "m4_table20", 15, 0, "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER },
			{ "m4_table21", 3, 1, "id", "name", "flag", INTEGER, STRING, BOOLEAN },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "M4_TABLE02", 3, 0, "m4_table01", 3, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0, "t", 3, 0 },
			null,
			null,
			null,
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "M4_TABLE02", 3, 0, "m4_table01", 3, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0, "t", 3, 0 },
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "M4_TABLE02", 3, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0, "t", 3, 0 },
			{ "m4_table01", 1, 0, "ps", STRING },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "M4_TABLE02", 3, 0, "m4_table01", 1, 0, "m4_table03", 3, 0, "m4_table04", 3, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0, "t", 3, 0 },
			null,
			null,
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			null,
			null,
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			null,
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 },
			null,
			{ "_range", 1, 0, "number", INTEGER, 0, 1, 2, 3, 4 },
			{ "m4_table01", 1, 0, "ps", STRING },
			null,
			null,
			{ "_tables", 3, 0, "table_name", "column_count", "row_count", STRING, INTEGER, INTEGER, "m4_table01", 1, 0, "m4_table05", 3, 0, "m4_table06", 3, 0, "m4_table10_____", 1, 0, "m4_table18", 1, 0, "m4_table19", 2, 0, "m4_table20", 15, 0, "m4_table21", 3, 0 }
		};
	}
}