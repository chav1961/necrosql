package chav1961.necrosql;

import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import chav1961.necrosql.exceptions.NecroSQLLowLayerException;
import chav1961.necrosql.interfaces.LowLayerInterface;
import chav1961.necrosql.interfaces.LowLayerInterface.FieldDesc;

class SimpleMetaData implements DatabaseMetaData {
	private static final ResultSetMetaData	MD_PROCEDURES = new SimpleResultSetMetadata(
													 new MetaDataRecord("PROCEDURE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("PROCEDURE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("PROCEDURE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("RESERVED1",MetaDataType.CHAR_TYPE,1)
													,new MetaDataRecord("RESERVED2",MetaDataType.CHAR_TYPE,1)
													,new MetaDataRecord("RESERVED3",MetaDataType.CHAR_TYPE,1)
													,new MetaDataRecord("REMARKS",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("PROCEDURE_TYPE",MetaDataType.NUMBER_TYPE,5,0)
													,new MetaDataRecord("SPECIFIC_NAME",MetaDataType.CHAR_TYPE,255)
													);
	private static final ResultSetMetaData	MD_PROCEDURE_COLUMNS = new SimpleResultSetMetadata(
													 new MetaDataRecord("PROCEDURE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("PROCEDURE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("PROCEDURE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("COLUMN_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("COLUMN_TYPE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("DATA_TYPE",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("TYPE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("PRECISION",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("LENGTH",MetaDataType.NUMBER_TYPE,3)
													,new MetaDataRecord("SCALE",MetaDataType.NUMBER_TYPE,1)
													,new MetaDataRecord("RADIX",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("NULLABLE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("REMARKS",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("COLUMN_DEF",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("SQL_DATA_TYPE",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("SQL_DATETIME_SUB",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("CHAR_OCTET_LENGTH",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("ORDINAL_POSITION",MetaDataType.NUMBER_TYPE,3)
													,new MetaDataRecord("IS_NULLABLE",MetaDataType.CHAR_TYPE,32)
													,new MetaDataRecord("SPECIFIC_NAME",MetaDataType.CHAR_TYPE,255)
													);
	private static final ResultSetMetaData	MD_SCHEMAS = new SimpleResultSetMetadata(
													 new MetaDataRecord("TABLE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("TABLE_CATALOG",MetaDataType.CHAR_TYPE,255)
													);
	private static final ResultSetMetaData	MD_CATALOGS = new SimpleResultSetMetadata(
													 new MetaDataRecord("TABLE_CAT",MetaDataType.CHAR_TYPE,255)
													);
	private static final ResultSetMetaData	MD_TABLETYPES  = new SimpleResultSetMetadata(
													 new MetaDataRecord("TABLE_TYPE",MetaDataType.CHAR_TYPE,10)
													);
	private static final ResultSetMetaData	MD_TABLES = new SimpleResultSetMetadata(
													 new MetaDataRecord("TABLE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("TABLE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("TABLE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("TABLE_TYPE",MetaDataType.CHAR_TYPE,20)
													,new MetaDataRecord("REMARKS",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("TYPE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("TYPE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("TYPE_NAME",MetaDataType.CHAR_TYPE,20)
													,new MetaDataRecord("SELF_REFERENCING_COL_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("REF_GENERATION",MetaDataType.CHAR_TYPE,10)
													);
	private static final ResultSetMetaData	MD_COLUMNS = new SimpleResultSetMetadata(
													 new MetaDataRecord("TABLE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("TABLE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("TABLE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("COLUMN_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("DATA_TYPE",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("TYPE_NAME",MetaDataType.CHAR_TYPE,20)
													,new MetaDataRecord("COLUMN_SIZE",MetaDataType.NUMBER_TYPE,3)
													,new MetaDataRecord("BUFFER_LENGTH",MetaDataType.NUMBER_TYPE,1)
													,new MetaDataRecord("DECIMAL_DIGITS",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("NUM_PREC_RADIX",MetaDataType.CHAR_TYPE,2)
													,new MetaDataRecord("NULLABLE",MetaDataType.NUMBER_TYPE,1)
													,new MetaDataRecord("REMARKS",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("COLUMN_DEF",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("SQL_DATA_TYPE",MetaDataType.NUMBER_TYPE,255)
													,new MetaDataRecord("SQL_DATETIME_SUB",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("CHAR_OCTET_LENGTH",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("ORDINAL_POSITION",MetaDataType.NUMBER_TYPE,3)
													,new MetaDataRecord("IS_NULLABLE",MetaDataType.CHAR_TYPE,3)
													,new MetaDataRecord("SCOPE_CATALOG",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("SCOPE_SCHEMA",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("SCOPE_TABLE",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("SOURCE_DATA_TYPE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("IS_AUTOINCREMENT",MetaDataType.CHAR_TYPE,3)
													,new MetaDataRecord("IS_GENERATEDCOLUMN",MetaDataType.CHAR_TYPE,3)
													);
	private static final ResultSetMetaData	MD_COLUMN_PRIVILEGES = new SimpleResultSetMetadata(
													 new MetaDataRecord("TABLE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("TABLE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("TABLE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("COLUMN_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("GRANTOR",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("GRANTEE",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("PRIVILEGE",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("IS_GRANTABLE",MetaDataType.CHAR_TYPE,3)
													);
	private static final ResultSetMetaData	MD_TABLE_PRIVILEGES = new SimpleResultSetMetadata(
													 new MetaDataRecord("TABLE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("TABLE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("TABLE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("GRANTOR",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("GRANTEE",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("PRIVILEGE",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("IS_GRANTABLE",MetaDataType.CHAR_TYPE,3)
													);
	private static final ResultSetMetaData	MD_BEST_ROW_IDENTIFIERS = new SimpleResultSetMetadata(
													 new MetaDataRecord("SCOPE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("COLUMN_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("DATA_TYPE",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("TYPE_NAME",MetaDataType.CHAR_TYPE,20)
													,new MetaDataRecord("COLUMN_SIZE",MetaDataType.NUMBER_TYPE,3)
													,new MetaDataRecord("BUFFER_LENGTH",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("DECIMAL_DIGITS",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("PSEUDO_COLUMN",MetaDataType.NUMBER_TYPE,5)
													);
	private static final ResultSetMetaData	MD_VERSION_COLUMNS = new SimpleResultSetMetadata(
													 new MetaDataRecord("SCOPE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("COLUMN_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("DATA_TYPE",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("TYPE_NAME",MetaDataType.CHAR_TYPE,20)
													,new MetaDataRecord("COLUMN_SIZE",MetaDataType.NUMBER_TYPE,3)
													,new MetaDataRecord("BUFFER_LENGTH",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("DECIMAL_DIGITS",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("PSEUDO_COLUMN",MetaDataType.NUMBER_TYPE,5)
													);
	private static final ResultSetMetaData	MD_PRIMARY_KEYS = new SimpleResultSetMetadata(
													 new MetaDataRecord("TABLE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("TABLE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("TABLE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("COLUMN_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("KEY_SEQ",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("PK_NAME",MetaDataType.CHAR_TYPE,255)
													);
	private static final ResultSetMetaData	MD_IMPORTED_KEYS = new SimpleResultSetMetadata(
													 new MetaDataRecord("PKTABLE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("PKTABLE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("PKTABLE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("PKCOLUMN_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("FKTABLE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("FKTABLE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("FKTABLE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("FKCOLUMN_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("KEY_SEQ",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("UPDATE_RULE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("DELETE_RULE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("FK_NAME",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("PK_NAME",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("DEFERRABILITY",MetaDataType.NUMBER_TYPE,5)
													);
	private static final ResultSetMetaData	MD_EXPORTED_KEYS = new SimpleResultSetMetadata(
													 new MetaDataRecord("PKTABLE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("PKTABLE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("PKTABLE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("PKCOLUMN_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("FKTABLE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("FKTABLE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("FKTABLE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("FKCOLUMN_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("KEY_SEQ",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("UPDATE_RULE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("DELETE_RULE",MetaDataType.CHAR_TYPE,5)
													,new MetaDataRecord("FK_NAME",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("PK_NAME",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("DEFERRABILITY",MetaDataType.NUMBER_TYPE,5)
													);
	private static final ResultSetMetaData	MD_CROSS_REFERENCES = new SimpleResultSetMetadata(
													 new MetaDataRecord("PKTABLE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("PKTABLE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("PKTABLE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("PKCOLUMN_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("FKTABLE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("FKTABLE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("FKTABLE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("FKCOLUMN_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("KEY_SEQ",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("UPDATE_RULE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("DELETE_RULE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("FK_NAME",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("PK_NAME",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("DEFERRABILITY",MetaDataType.NUMBER_TYPE,5)
													);
	private static final ResultSetMetaData	MD_TYPE_INFO = new SimpleResultSetMetadata(
													 new MetaDataRecord("TYPE_NAME",MetaDataType.CHAR_TYPE,20)
													,new MetaDataRecord("DATA_TYPE",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("PRECISION",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("LITERAL_PREFIX",MetaDataType.CHAR_TYPE,1)
													,new MetaDataRecord("LITERAL_SUFFIX",MetaDataType.CHAR_TYPE,1)
													,new MetaDataRecord("CREATE_PARAMS",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("NULLABLE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("CASE_SENSITIVE",MetaDataType.LOGICAL_TYPE,1)
													,new MetaDataRecord("SEARCHABLE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("UNSIGNED_ATTRIBUTE",MetaDataType.LOGICAL_TYPE,1)
													,new MetaDataRecord("FIXED_PREC_SCALE",MetaDataType.LOGICAL_TYPE,1)
													,new MetaDataRecord("AUTO_INCREMENT",MetaDataType.LOGICAL_TYPE,1)
													,new MetaDataRecord("LOCAL_TYPE_NAME",MetaDataType.CHAR_TYPE,20)
													,new MetaDataRecord("MINIMUM_SCALE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("MAXIMUM_SCALE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("SQL_DATA_TYPE",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("SQL_DATETIME_SUB",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("NUM_PREC_RADIX",MetaDataType.NUMBER_TYPE,10)
													);
	private static final ResultSetMetaData	MD_INDEX_INFO = new SimpleResultSetMetadata(
													 new MetaDataRecord("TABLE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("TABLE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("TABLE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("NON_UNIQUE",MetaDataType.LOGICAL_TYPE,1)
													,new MetaDataRecord("INDEX_QUALIFIER",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("INDEX_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("TYPE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("ORDINAL_POSITION",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("COLUMN_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("ASC_OR_DESC",MetaDataType.CHAR_TYPE,1)
													,new MetaDataRecord("CARDINALITY",MetaDataType.NUMBER_TYPE,20)
													,new MetaDataRecord("PAGES",MetaDataType.NUMBER_TYPE,20)
													,new MetaDataRecord("FILTER_CONDITION",MetaDataType.CHAR_TYPE,255)
													);
	private static final ResultSetMetaData	MD_UDTS = new SimpleResultSetMetadata(
													 new MetaDataRecord("TYPE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("TYPE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("TYPE_NAME",MetaDataType.CHAR_TYPE,20)
													,new MetaDataRecord("CLASS_NAME",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("DATA_TYPE",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("REMARKS",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("BASE_TYPE",MetaDataType.NUMBER_TYPE,5)
													);
	private static final ResultSetMetaData	MD_SUPERTYPES = new SimpleResultSetMetadata(
													 new MetaDataRecord("TYPE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("TYPE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("TYPE_NAME",MetaDataType.CHAR_TYPE,20)
													,new MetaDataRecord("SUPERTYPE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("SUPERTYPE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("SUPERTYPE_NAME",MetaDataType.CHAR_TYPE,20)
													);
	private static final ResultSetMetaData	MD_SUPERTABLES = new SimpleResultSetMetadata(
													 new MetaDataRecord("TABLE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("TABLE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("TABLE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("SUPERTABLE_NAME",MetaDataType.CHAR_TYPE,10)
													);
	private static final ResultSetMetaData	MD_ATTRIBUTES = new SimpleResultSetMetadata(
													 new MetaDataRecord("TYPE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("TYPE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("TYPE_NAME",MetaDataType.CHAR_TYPE,20)
													,new MetaDataRecord("ATTR_NAME",MetaDataType.CHAR_TYPE,20)
													,new MetaDataRecord("DATA_TYPE",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("ATTR_TYPE_NAME",MetaDataType.CHAR_TYPE,20)
													,new MetaDataRecord("ATTR_SIZE",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("DECIMAL_DIGITS",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("NUM_PREC_RADIX",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("NULLABLE",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("REMARKS",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("ATTR_DEF",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("SQL_DATA_TYPE",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("SQL_DATETIME_SUB",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("CHAR_OCTET_LENGTH",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("ORDINAL_POSITION",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("IS_NULLABLE",MetaDataType.CHAR_TYPE,3)
													,new MetaDataRecord("SCOPE_CATALOG",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("SCOPE_SCHEMA",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("SCOPE_TABLE",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("SOURCE_DATA_TYPE",MetaDataType.NUMBER_TYPE,5)
													);
	private static final ResultSetMetaData	MD_CLIENTINFO_PROPERTIES = new SimpleResultSetMetadata(
													 new MetaDataRecord("NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("MAX_LEN",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("DEFAULT_VALUE",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("DESCRIPTION",MetaDataType.CHAR_TYPE,255)
													);
	private static final ResultSetMetaData	MD_FUNCTIONS = new SimpleResultSetMetadata(
													 new MetaDataRecord("FUNCTION_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("FUNCTION_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("FUNCTION_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("REMARKS",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("FUNCTION_TYPE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("SPECIFIC_NAME",MetaDataType.CHAR_TYPE,255)
													);
	private static final ResultSetMetaData	MD_FUNCTION_COLUMNS = new SimpleResultSetMetadata(
													 new MetaDataRecord("FUNCTION_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("FUNCTION_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("FUNCTION_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("COLUMN_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("COLUMN_TYPE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("DATA_TYPE",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("TYPE_NAME",MetaDataType.CHAR_TYPE,20)
													,new MetaDataRecord("PRECISION",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("LENGTH",MetaDataType.NUMBER_TYPE,3)
													,new MetaDataRecord("SCALE",MetaDataType.NUMBER_TYPE,3)
													,new MetaDataRecord("RADIX",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("NULLABLE",MetaDataType.NUMBER_TYPE,5)
													,new MetaDataRecord("REMARKS",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("CHAR_OCTET_LENGTH",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("ORDINAL_POSITION",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("IS_NULLABLE",MetaDataType.CHAR_TYPE,3)
													,new MetaDataRecord("SPECIFIC_NAME",MetaDataType.CHAR_TYPE,255)
													);
	private static final ResultSetMetaData	MD_PSEUDOCOLUMNS = new SimpleResultSetMetadata(
													 new MetaDataRecord("TABLE_CAT",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("TABLE_SCHEM",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("TABLE_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("COLUMN_NAME",MetaDataType.CHAR_TYPE,10)
													,new MetaDataRecord("DATA_TYPE",MetaDataType.NUMBER_TYPE,10)
													,new MetaDataRecord("COLUMN_SIZE",MetaDataType.NUMBER_TYPE,3)
													,new MetaDataRecord("DECIMAL_DIGITS",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("NUM_PREC_RADIX",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("COLUMN_USAGE",MetaDataType.CHAR_TYPE,20)
													,new MetaDataRecord("REMARKS",MetaDataType.CHAR_TYPE,255)
													,new MetaDataRecord("CHAR_OCTET_LENGTH",MetaDataType.NUMBER_TYPE,2)
													,new MetaDataRecord("IS_NULLABLE",MetaDataType.CHAR_TYPE,3)
													);
	private static final ConversionDesc[]		CONVERSIONS = new ConversionDesc[]{
													 new ConversionDesc(Types.BIT,Types.BIT,Types.SMALLINT,Types.INTEGER,Types.BIGINT,Types.REAL,Types.DOUBLE,Types.NUMERIC,Types.CHAR,Types.VARCHAR,Types.DATE,Types.JAVA_OBJECT,Types.CLOB,Types.BOOLEAN)
													,new ConversionDesc(Types.SMALLINT,Types.BIT,Types.SMALLINT,Types.INTEGER,Types.BIGINT,Types.REAL,Types.DOUBLE,Types.NUMERIC,Types.CHAR,Types.VARCHAR,Types.DATE,Types.JAVA_OBJECT,Types.CLOB,Types.BOOLEAN)
													,new ConversionDesc(Types.INTEGER,Types.BIT,Types.SMALLINT,Types.INTEGER,Types.BIGINT,Types.REAL,Types.DOUBLE,Types.NUMERIC,Types.CHAR,Types.VARCHAR,Types.DATE,Types.JAVA_OBJECT,Types.CLOB,Types.BOOLEAN)
													,new ConversionDesc(Types.BIGINT,Types.BIT,Types.SMALLINT,Types.INTEGER,Types.BIGINT,Types.REAL,Types.DOUBLE,Types.NUMERIC,Types.CHAR,Types.VARCHAR,Types.DATE,Types.JAVA_OBJECT,Types.CLOB,Types.BOOLEAN)
													,new ConversionDesc(Types.REAL,Types.BIT,Types.SMALLINT,Types.INTEGER,Types.BIGINT,Types.REAL,Types.DOUBLE,Types.NUMERIC,Types.CHAR,Types.VARCHAR,Types.DATE,Types.JAVA_OBJECT,Types.CLOB,Types.BOOLEAN)
													,new ConversionDesc(Types.DOUBLE,Types.BIT,Types.SMALLINT,Types.INTEGER,Types.BIGINT,Types.REAL,Types.DOUBLE,Types.NUMERIC,Types.CHAR,Types.VARCHAR,Types.DATE,Types.JAVA_OBJECT,Types.CLOB,Types.BOOLEAN)
													,new ConversionDesc(Types.NUMERIC,Types.BIT,Types.SMALLINT,Types.INTEGER,Types.BIGINT,Types.REAL,Types.DOUBLE,Types.NUMERIC,Types.CHAR,Types.VARCHAR,Types.DATE,Types.JAVA_OBJECT,Types.CLOB,Types.BOOLEAN)
													,new ConversionDesc(Types.CHAR,Types.BIT,Types.SMALLINT,Types.INTEGER,Types.BIGINT,Types.REAL,Types.DOUBLE,Types.NUMERIC,Types.CHAR,Types.VARCHAR,Types.DATE,Types.JAVA_OBJECT,Types.CLOB,Types.BOOLEAN)
													,new ConversionDesc(Types.DATE,Types.BIT,Types.SMALLINT,Types.INTEGER,Types.BIGINT,Types.REAL,Types.DOUBLE,Types.NUMERIC,Types.CHAR,Types.VARCHAR,Types.DATE,Types.JAVA_OBJECT,Types.CLOB,Types.BOOLEAN)
													,new ConversionDesc(Types.JAVA_OBJECT,Types.BIT,Types.SMALLINT,Types.INTEGER,Types.BIGINT,Types.REAL,Types.DOUBLE,Types.NUMERIC,Types.CHAR,Types.VARCHAR,Types.DATE,Types.JAVA_OBJECT,Types.CLOB,Types.BOOLEAN)
													,new ConversionDesc(Types.CLOB,Types.BIT,Types.SMALLINT,Types.INTEGER,Types.BIGINT,Types.REAL,Types.DOUBLE,Types.NUMERIC,Types.CHAR,Types.VARCHAR,Types.DATE,Types.JAVA_OBJECT,Types.CLOB,Types.BOOLEAN)
													,new ConversionDesc(Types.BOOLEAN,Types.BIT,Types.SMALLINT,Types.INTEGER,Types.BIGINT,Types.REAL,Types.DOUBLE,Types.NUMERIC,Types.CHAR,Types.VARCHAR,Types.DATE,Types.JAVA_OBJECT,Types.CLOB,Types.BOOLEAN)
													};
	private static final Object[][]				TYPE_INFO = new Object[][]{
														 new Object[]{
																 "CHARACTER"		// TYPE_NAME 
																 ,Types.CHAR		// DATA_TYPE 
																 ,0					// PRECISION 
																 ,""				// LITERAL_PREFIX 
																 ,""				// LITERAL_SUFFIX 
																 ,"(size)"			// CREATE_PARAMS 
																 ,typeNoNulls		// NULLABLE 
																 ,true				// CASE_SENSITIVE 
																 ,typeSearchable	// SEARCHABLE 
																 ,true				// UNSIGNED_ATTRIBUTE 
																 ,false				// FIXED_PREC_SCALE 
																 ,false				// AUTO_INCREMENT 
																 ,"CHARACTER"		// LOCAL_TYPE_NAME 
																 ,0					// MINIMUM_SCALE 
																 ,0					// MAXIMUM_SCALE 
																 ,0					// SQL_DATA_TYPE 
																 ,0					// SQL_DATETIME_SUB 
																 ,0					// NUM_PREC_RADIX 
														 }
														,new Object[]{
																 "NUMERIC"			// TYPE_NAME 
																 ,Types.NUMERIC		// DATA_TYPE 
																 ,10				// PRECISION 
																 ,""				// LITERAL_PREFIX 
																 ,""				// LITERAL_SUFFIX 
																 ,"(size[,prec])"	// CREATE_PARAMS 
																 ,typeNoNulls		// NULLABLE 
																 ,false				// CASE_SENSITIVE 
																 ,typeSearchable	// SEARCHABLE 
																 ,false				// UNSIGNED_ATTRIBUTE 
																 ,true				// FIXED_PREC_SCALE 
																 ,true				// AUTO_INCREMENT 
																 ,"NUMERIC"			// LOCAL_TYPE_NAME 
																 ,-300				// MINIMUM_SCALE 
																 ,300				// MAXIMUM_SCALE 
																 ,0					// SQL_DATA_TYPE 
																 ,0					// SQL_DATETIME_SUB 
																 ,10				// NUM_PREC_RADIX 
														 }
														,new Object[]{
															 "DATE"				// TYPE_NAME 
															 ,Types.DATE		// DATA_TYPE 
															 ,0					// PRECISION 
															 ,""				// LITERAL_PREFIX 
															 ,""				// LITERAL_SUFFIX 
															 ,""				// CREATE_PARAMS 
															 ,typeNoNulls		// NULLABLE 
															 ,false				// CASE_SENSITIVE 
															 ,typeSearchable	// SEARCHABLE 
															 ,false				// UNSIGNED_ATTRIBUTE 
															 ,false				// FIXED_PREC_SCALE 
															 ,false				// AUTO_INCREMENT 
															 ,"DATE"			// LOCAL_TYPE_NAME 
															 ,0					// MINIMUM_SCALE 
															 ,0					// MAXIMUM_SCALE 
															 ,0					// SQL_DATA_TYPE 
															 ,0					// SQL_DATETIME_SUB 
															 ,0					// NUM_PREC_RADIX 
														 }
														,new Object[]{
															 "BOOLEAN"			// TYPE_NAME 
															 ,Types.BOOLEAN		// DATA_TYPE 
															 ,0					// PRECISION 
															 ,""				// LITERAL_PREFIX 
															 ,""				// LITERAL_SUFFIX 
															 ,""				// CREATE_PARAMS 
															 ,typeNoNulls		// NULLABLE 
															 ,false				// CASE_SENSITIVE 
															 ,typeSearchable	// SEARCHABLE 
															 ,false				// UNSIGNED_ATTRIBUTE 
															 ,false				// FIXED_PREC_SCALE 
															 ,false				// AUTO_INCREMENT 
															 ,"BOOLEAN"			// LOCAL_TYPE_NAME 
															 ,0					// MINIMUM_SCALE 
															 ,0					// MAXIMUM_SCALE 
															 ,0					// SQL_DATA_TYPE 
															 ,0					// SQL_DATETIME_SUB 
															 ,0					// NUM_PREC_RADIX 
														 }
														,new Object[]{
															 "CLOB"				// TYPE_NAME 
															 ,Types.CLOB		// DATA_TYPE 
															 ,0					// PRECISION 
															 ,""				// LITERAL_PREFIX 
															 ,""				// LITERAL_SUFFIX 
															 ,""				// CREATE_PARAMS 
															 ,typeNoNulls		// NULLABLE 
															 ,false				// CASE_SENSITIVE 
															 ,typePredNone		// SEARCHABLE 
															 ,false				// UNSIGNED_ATTRIBUTE 
															 ,false				// FIXED_PREC_SCALE 
															 ,false				// AUTO_INCREMENT 
															 ,"CLOB"			// LOCAL_TYPE_NAME 
															 ,0					// MINIMUM_SCALE 
															 ,0					// MAXIMUM_SCALE 
															 ,0					// SQL_DATA_TYPE 
															 ,0					// SQL_DATETIME_SUB 
															 ,0					// NUM_PREC_RADIX 
													 }
													}; 
	
	private final File				root;
	private final SimpleConnection	conn;
	private final LowLayerInterface	lli;
	

	SimpleMetaData(final File root, final SimpleConnection conn, final LowLayerInterface lli) {
		this.root = root;
		this.conn = conn;
		this.lli = lli;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("This database doesn't support wrappers for data");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return true;
	}

	@Override
	public String getURL() throws SQLException {
		return null;
	}

	@Override
	public String getUserName() throws SQLException {
		return conn.getClientInfo(SimpleDriver.PROP_USER);
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return conn.isReadOnly();
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		return true;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		return true;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return false;
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return SimpleDriver.DRIVER_PRODUCT;
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return SimpleDriver.DRIVER_MAJ_VER+"."+SimpleDriver.DRIVER_MIN_VER;
	}

	@Override
	public String getDriverName() throws SQLException {
		return SimpleDriver.DRIVER_NAME;
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return SimpleDriver.DRIVER_MAJ_VER+"."+SimpleDriver.DRIVER_MIN_VER;
	}

	@Override
	public int getDriverMajorVersion() {
		return SimpleDriver.DRIVER_MAJ_VER;
	}

	@Override
	public int getDriverMinorVersion() {
		return SimpleDriver.DRIVER_MIN_VER;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		return true;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return "\"";
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		return "";
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getStringFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return "?*";
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		return "";
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return true;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType) throws SQLException {
		for (ConversionDesc item : CONVERSIONS) {
			if (item.fromType == fromType) {
				for (int value : item.toType) {
					if (value == toType) {
						return true;
					}
				}
				return false;
			}
		}
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return null;
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		return null;
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		return null;
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		return true;
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		return File.separator;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return true;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		return 10;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 10;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		return 10;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 10;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		return 1;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		return 10;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		return 1024;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		return 10;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		return 10;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		return 65535;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return false;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		return 16384;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		return 10;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		return 1;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
		return level == Connection.TRANSACTION_NONE;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
		return new SimpleResultSet(MD_PROCEDURES);
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
		return new SimpleResultSet(MD_PROCEDURE_COLUMNS);
	}

	@Override
	public ResultSet getTables(final String catalog, final String schemaPattern, final String tableNamePattern, final String[] types) throws SQLException {
		if (types == null || "TABLE".equalsIgnoreCase(types[0])) {
			final File[]		tables = root.listFiles(new FilenameFilter(){
												@Override
												public boolean accept(File dir, String name) {
													return name.toUpperCase().endsWith(".dbf")
															&& (tableNamePattern == null || 
																NecroUtils.like(name.toUpperCase().substring(0,name.length()-4),tableNamePattern)
																);
												}
											}
										);
			final Object[][]	result = new Object[tables.length][];
			
			for (int index = 0; index < tables.length; index++) {
				result[index] = new Object[]{root.getAbsolutePath().replace(File.separatorChar,'/')		// TABLE_CAT 
											, Null.instance												// TABLE_SCHEM 
											, tables[index].getName().toUpperCase().replace(".DBF","")	// TABLE_NAME 
											, "TABLE"													// TABLE_TYPE 
											, tables[index].getName()									// REMARKS 
											, Null.instance												// TYPE_CAT 
											, Null.instance												// TYPE_SCHEM 
											, Null.instance												// TYPE_NAME 
											, Null.instance												// SELF_REFERENCING_COL_NAME 
											, Null.instance												// REF_GENERATION 
											};
			}
			return new SimpleResultSet(new PredefinedRowsCollection(result,true),MD_TABLES);
		}
		else {
			return new SimpleResultSet(MD_TABLES);
		}
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		return new SimpleResultSet(MD_SCHEMAS);
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		return new SimpleResultSet(new PredefinedRowsCollection(new Object[][]{new Object[]{root.getAbsolutePath().replace(File.separatorChar,'/')}},true),MD_CATALOGS);
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		return new SimpleResultSet(new PredefinedRowsCollection(new Object[][]{new Object[]{"TABLE"}},true),MD_TABLETYPES);
	}

	@Override
	public ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern, final String columnNamePattern) throws SQLException {
		final List<Object[]>	result = new ArrayList<>();
		
		try(final ResultSet	rs = getTables(catalog,schemaPattern,tableNamePattern,new String[]{"TABLE"})) {
			while (rs.next()) {
				int		count = 1;
				
				for (FieldDesc item : lli.getFieldDesc(rs.getString("TABLE_NAME"))) {
					if (columnNamePattern == null || NecroUtils.like(item.getName(),columnNamePattern)) {
						result.add(new Object[]{
								 rs.getString("TABLE_CAT")
								,rs.getString("TABLE_SCHEM")
								,rs.getString("TABLE_NAME")
								,item.getName()								// COLUMN_NAME
								,item.getType().getVendorTypeNumber()		// DATA_TYPE
								,item.getType().getName()					// TYPE_NAME 
								,item.getSize()								// COLUMN_SIZE
								,0											// BUFFER_LENGTH
								,item.getPrecision()						// DECIMAL_DIGITS
								,10											// NUM_PREC_RADIX
								,columnNoNulls								// NULLABLE
								,item.getName()								// REMARKS
								,""											// COLUMN_DEF
								,item.getType().getVendorTypeNumber()		// SQL_DATA_TYPE
								,0											// SQL_DATETIME_SUB
								,0											// CHAR_OCTET_LENGTH
								,count										// ORDINAL_POSITION
								,"NO"										// IS_NULLABLE
								,Null.instance								// SCOPE_CATALOG
								,Null.instance 								// SCOPE_SCHEMA
								,Null.instance								// SCOPE_TABLE
								,item.getType().getVendorTypeNumber()		// SOURCE_DATA_TYPE
								,"NO"										// IS_AUTOINCREMENT
								,"NO"										// IS_GENERATEDCOLUMN
							}
						);
					}
					count++;
				}
			}
			return new SimpleResultSet(new PredefinedRowsCollection(result.toArray(new Object[result.size()][]),true),MD_COLUMNS);
		} catch (NecroSQLLowLayerException e) {
			throw new SQLException("Low layer interface problem was detected: "+e.getMessage());
		}
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
		return new SimpleResultSet(MD_COLUMN_PRIVILEGES);
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		return new SimpleResultSet(MD_TABLE_PRIVILEGES);
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
		return new SimpleResultSet(MD_BEST_ROW_IDENTIFIERS);
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
		return new SimpleResultSet(MD_VERSION_COLUMNS);
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		return new SimpleResultSet(MD_PRIMARY_KEYS);
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		return new SimpleResultSet(MD_IMPORTED_KEYS);
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
		return new SimpleResultSet(MD_EXPORTED_KEYS);
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
		return new SimpleResultSet(MD_CROSS_REFERENCES);
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		return new SimpleResultSet(new PredefinedRowsCollection(TYPE_INFO,true),MD_TYPE_INFO);
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
		return new SimpleResultSet(MD_INDEX_INFO);
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		return type == ResultSet.TYPE_FORWARD_ONLY || type == ResultSet.TYPE_SCROLL_SENSITIVE || type == ResultSet.TYPE_SCROLL_INSENSITIVE;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
		return true;
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		return true;
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		return true;
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		return true;
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		return true;
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		return true;
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		return true;
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		return true;
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		return true;
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		return true;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
		return new SimpleResultSet(MD_UDTS);
	}

	@Override
	public Connection getConnection() throws SQLException {
		return conn;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
		return new SimpleResultSet(MD_SUPERTYPES);
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		return new SimpleResultSet(MD_SUPERTABLES);
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
		return new SimpleResultSet(MD_ATTRIBUTES);
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) throws SQLException {
		return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		return 3;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return 0;
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return 3;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return 0;
	}

	@Override
	public int getSQLStateType() throws SQLException {
		return sqlStateSQL;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		return false;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		return new SimpleResultSet(MD_SCHEMAS);
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		return new SimpleResultSet(MD_CLIENTINFO_PROPERTIES);
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
		return new SimpleResultSet(MD_FUNCTIONS);
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
		return new SimpleResultSet(MD_FUNCTION_COLUMNS);
	}

	@Override
	public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
		return new SimpleResultSet(MD_PSEUDOCOLUMNS);
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		return false;
	}
	
	private static class ConversionDesc {
		final int	fromType;
		final int[]	toType;
		public ConversionDesc(int fromType, int... toType) {
			this.fromType = fromType;
			this.toType = toType;
		}
	}
}
