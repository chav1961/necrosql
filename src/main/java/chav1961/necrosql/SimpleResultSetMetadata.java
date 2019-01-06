package chav1961.necrosql;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;

import chav1961.necrosql.interfaces.LowLayerInterface.FieldDesc;

class SimpleResultSetMetadata implements ResultSetMetaData {
	private final File				dirLocation;
	private final String			table;
	private final boolean			readOnly;
	private final FieldDesc[]		desc;
	
	SimpleResultSetMetadata(final FieldDesc... descriptors) {
		this(null,null,true,descriptors);
	}
	
	SimpleResultSetMetadata(final File dirLocation, final FieldDesc... descriptors) {
		this(dirLocation,null,true,descriptors);
	}
	
	SimpleResultSetMetadata(final File dirLocation, final String table, final boolean readOnly, final FieldDesc... descriptors) {
		this.dirLocation = dirLocation;
		this.table = table;
		this.readOnly = readOnly;
		this.desc = descriptors;
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
	public int getColumnCount() throws SQLException {
		return desc.length;
	}

	@Override
	public boolean isAutoIncrement(final int column) throws SQLException {
		checkIndex(column);
		return false;
	}

	@Override
	public boolean isCaseSensitive(final int column) throws SQLException {
		checkIndex(column);
		return false;
	}

	@Override
	public boolean isSearchable(final int column) throws SQLException {
		checkIndex(column);
		return false;
	}

	@Override
	public boolean isCurrency(final int column) throws SQLException {
		return getColumnType(column) == MetaDataType.NUMBER_LETTER;
	}

	@Override
	public int isNullable(final int column) throws SQLException {
		checkIndex(column);
		return 0;
	}

	@Override
	public boolean isSigned(final int column) throws SQLException {
		return getColumnType(column) == MetaDataType.NUMBER_LETTER;
	}

	@Override
	public int getColumnDisplaySize(final int column) throws SQLException {
		return desc[checkIndex(column)].getSize();
	}

	@Override
	public String getColumnLabel(final int column) throws SQLException {
		return getColumnName(column);
	}

	@Override
	public String getColumnName(final int column) throws SQLException {
		return desc[checkIndex(column)].getName();
	}

	@Override
	public String getSchemaName(final int column) throws SQLException {
		checkIndex(column);
		return null;
	}

	@Override
	public int getPrecision(final int column) throws SQLException {
		return desc[checkIndex(column)].getPrecision();
	}

	@Override
	public int getScale(final int column) throws SQLException {
		checkIndex(column);
		return 0;
	}

	@Override
	public String getTableName(final int column) throws SQLException {
		checkIndex(column);
		return table;
	}

	@Override
	public String getCatalogName(final int column) throws SQLException {
		checkIndex(column);
		return dirLocation.getAbsolutePath().replace(File.separatorChar,'/');
	}

	@Override
	public int getColumnType(final int column) throws SQLException {
		return desc[checkIndex(column)].getType().getVendorTypeNumber();
	}

	@Override
	public String getColumnTypeName(final int column) throws SQLException {
		return desc[checkIndex(column)].getType().getName();
	}

	@Override
	public boolean isReadOnly(final int column) throws SQLException {
		checkIndex(column);
		return readOnly;
	}

	@Override
	public boolean isWritable(final int column) throws SQLException {
		checkIndex(column);
		return !readOnly;
	}

	@Override
	public boolean isDefinitelyWritable(final int column) throws SQLException {
		checkIndex(column);
		return !readOnly;
	}

	@Override
	public String getColumnClassName(final int column) throws SQLException {
		switch (getColumnType(column)) {
			case MetaDataType.CHAR_LETTER 		:	return String.class.getName();
			case MetaDataType.CLOB_LETTER 		:	return char[].class.getCanonicalName();
			case MetaDataType.DATE_LETTER 		:	return Date.class.getName();
			case MetaDataType.LOGICAL_LETTER	:	return boolean.class.getCanonicalName();
			case MetaDataType.NUMBER_LETTER		:	return BigDecimal.class.getName();
			default : throw new UnsupportedOperationException("Column ["+getColumnName(column)+"] doesn't have type class assigned");
		}
	}

	@Override
	public String toString() {
		return "SimpleResultSetMetadata [dirLocation=" + dirLocation + ", table=" + table + ", readOnly=" + readOnly + ", desc=" + Arrays.toString(desc) + "]";
	}

	private int checkIndex(int column) {
		if (column < 1 || column > desc.length) {
			throw new IllegalArgumentException("Column index ["+column+"] out of range 1.."+desc.length);
		}
		else {
			return column - 1;
		}
	}
}
