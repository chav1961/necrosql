package chav1961.necrosql;


import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import chav1961.necrosql.interfaces.RowsCollection;

class SimpleResultSet implements ResultSet {
	private final String			cursorName;
	private final ResultSetMetaData	metadata;
	private final Statement			statement;
	private final int				cursorType;
	private final int				cursorConcurrency;
	private final int				cursorHoldability;
	private RowsCollection			content;

	private boolean			isClosed = false;
	private int				fetchDirection = FETCH_FORWARD;
	private int				fetchSize = 1;
	private int				cursorIndex;

	SimpleResultSet(final ResultSetMetaData metadata) {
		this.cursorName = "";
		this.metadata = metadata;
		this.statement = null;
		this.cursorType = TYPE_FORWARD_ONLY;
		this.cursorConcurrency = CONCUR_READ_ONLY;
		this.cursorHoldability = HOLD_CURSORS_OVER_COMMIT;
		this.cursorIndex = -1;
		this.content = new PredefinedRowsCollection(true);
	}

	SimpleResultSet(final RowsCollection content,final ResultSetMetaData metadata) {
		this.cursorName = "";
		this.metadata = metadata;
		this.statement = null;
		this.cursorType = TYPE_FORWARD_ONLY;
		this.cursorConcurrency = CONCUR_READ_ONLY;
		this.cursorHoldability = HOLD_CURSORS_OVER_COMMIT;
		this.cursorIndex = -1;
		this.content = content;
	}
	
	@Override
	public <T> T unwrap(final Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("This database doesn't support wrappers for data");
	}

	@Override
	public boolean isWrapperFor(final Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public boolean next() throws SQLException {
		checkClosed();
		return ++cursorIndex < content.size();
	}

	@Override
	public void close() throws SQLException {
		if (isClosed) {
			throw new SQLException("Duplicate closure for the result set!");
		}
		else {
			isClosed = true;
			// TODO Auto-generated method stub
		}
	}

	@Override
	public boolean wasNull() throws SQLException {
		checkClosed();
		checkInsideDataArray();
		return false;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return "";
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return false;
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return 0;
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return 0;
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return 0;
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return 0;
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return 0;
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return 0;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return null;
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return null;
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return null;
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return null;
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return null;
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return null;
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return null;
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return null;
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		return getString(label2index(columnLabel));
	}

	@Override
	public boolean getBoolean(final String columnLabel) throws SQLException {
		return getBoolean(label2index(columnLabel));
	}

	@Override
	public byte getByte(final String columnLabel) throws SQLException {
		return getByte(label2index(columnLabel));
	}

	@Override
	public short getShort(final String columnLabel) throws SQLException {
		return getShort(label2index(columnLabel));
	}

	@Override
	public int getInt(final String columnLabel) throws SQLException {
		return getInt(label2index(columnLabel));
	}

	@Override
	public long getLong(final String columnLabel) throws SQLException {
		return getLong(label2index(columnLabel));
	}

	@Override
	public float getFloat(final String columnLabel) throws SQLException {
		return getFloat(label2index(columnLabel));
	}

	@Override
	public double getDouble(final String columnLabel) throws SQLException {
		return getDouble(label2index(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
		return getBigDecimal(label2index(columnLabel),scale);
	}

	@Override
	public byte[] getBytes(final String columnLabel) throws SQLException {
		return getBytes(label2index(columnLabel));
	}

	@Override
	public Date getDate(final String columnLabel) throws SQLException {
		return getDate(label2index(columnLabel));
	}

	@Override
	public Time getTime(final String columnLabel) throws SQLException {
		return getTime(label2index(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(final String columnLabel) throws SQLException {
		return getTimestamp(label2index(columnLabel));
	}

	@Override
	public InputStream getAsciiStream(final String columnLabel) throws SQLException {
		return getAsciiStream(label2index(columnLabel));
	}

	@Override
	public InputStream getUnicodeStream(final String columnLabel) throws SQLException {
		return getUnicodeStream(label2index(columnLabel));
	}

	@Override
	public InputStream getBinaryStream(final String columnLabel) throws SQLException {
		return getBinaryStream(label2index(columnLabel));
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getCursorName() throws SQLException {
		return cursorName;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metadata;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return null;
	}

	@Override
	public Object getObject(final String columnLabel) throws SQLException {
		return getObject(label2index(columnLabel));
	}

	@Override
	public int findColumn(final String columnLabel) throws SQLException {
		return label2index(columnLabel);
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		checkClosed();
		checkColumn(columnIndex);		
		checkInsideDataArray();
		return null;
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		return getCharacterStream(label2index(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return getBigDecimal(columnIndex,0);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getBigDecimal(columnLabel,0);
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		checkClosed();
		return cursorIndex < 0;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		checkClosed();
		return cursorIndex >= content.size();
	}

	@Override
	public boolean isFirst() throws SQLException {
		checkClosed();
		return cursorIndex == 0;
	}

	@Override
	public boolean isLast() throws SQLException {
		checkClosed();
		return cursorIndex == content.size()-1;
	}

	@Override
	public void beforeFirst() throws SQLException {
		checkClosed();
		checkForwardOnly();
		cursorIndex = -1;
	}

	@Override
	public void afterLast() throws SQLException {
		checkClosed();
		checkForwardOnly();
		cursorIndex = content.size();
	}

	@Override
	public boolean first() throws SQLException {
		return absolute(0);
	}

	@Override
	public boolean last() throws SQLException {
		return absolute(content.size()-1);
	}

	@Override
	public int getRow() throws SQLException {
		checkClosed();
		return cursorIndex;
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		checkClosed();
		checkForwardOnly();
		if (row < 0 || row > content.size()) {
			return false;
		}
		else {
			cursorIndex = row;
			return true;
		}
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		return absolute(getRow()+rows);
	}

	@Override
	public boolean previous() throws SQLException {
		return relative(-1);
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		if (direction != FETCH_FORWARD && direction != FETCH_REVERSE && direction != FETCH_UNKNOWN) {
			throw new IllegalArgumentException("Illegal value ["+direction+"] for direction: availables are "+FETCH_FORWARD+" , "+FETCH_REVERSE+" and "+FETCH_UNKNOWN);
		}
		else {
			this.fetchDirection = direction;
		}
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return fetchDirection;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		if (rows <= 0) {
			throw new IllegalArgumentException("Fetch size value ["+rows+"] need be positive");
		}
		else {
			this.fetchSize = rows;
		}
	}

	@Override
	public int getFetchSize() throws SQLException {
		return fetchSize;
	}

	@Override
	public int getType() throws SQLException {
		return cursorType;
	}

	@Override
	public int getConcurrency() throws SQLException {
		return cursorConcurrency;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		checkClosed();
		checkInsideDataArray();
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		checkClosed();
		checkInsideDataArray();
		return false;
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		checkClosed();
		checkInsideDataArray();
		return false;
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkColumn(columnIndex);		
		checkInsideDataArray();
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		updateNull(label2index(columnLabel));
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		updateBoolean(label2index(columnLabel),x);
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		updateByte(label2index(columnLabel),x);
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		updateShort(label2index(columnLabel),x);
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		updateInt(label2index(columnLabel),x);
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		updateLong(label2index(columnLabel),x);
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		updateFloat(label2index(columnLabel),x);
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		updateDouble(label2index(columnLabel),x);
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		updateBigDecimal(label2index(columnLabel),x);
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		updateString(label2index(columnLabel),x);
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		updateBytes(label2index(columnLabel),x);
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		updateDate(label2index(columnLabel),x);
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		updateTime(label2index(columnLabel),x);
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		updateTimestamp(label2index(columnLabel),x);
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		updateAsciiStream(label2index(columnLabel),x,length);
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		updateBinaryStream(label2index(columnLabel),x,length);
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		updateCharacterStream(label2index(columnLabel),reader,length);
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		updateObject(label2index(columnLabel),x,scaleOrLength);
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		updateObject(label2index(columnLabel),x);
	}

	@Override
	public void insertRow() throws SQLException {
		checkClosed();
		checkReadOnly();
		checkForwardOnly();
	}

	@Override
	public void updateRow() throws SQLException {
		checkClosed();
		checkReadOnly();
		checkForwardOnly();
	}

	@Override
	public void deleteRow() throws SQLException {
		checkClosed();
		checkReadOnly();
		checkForwardOnly();
	}

	@Override
	public void refreshRow() throws SQLException {
		checkClosed();
		checkInsideDataArray();
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		checkClosed();
		checkReadOnly();
		checkForwardOnly();
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		checkClosed();
		checkInsideDataArray();
	}

	@Override
	public Statement getStatement() throws SQLException {
		return statement;
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		checkClosed();
		checkInsideDataArray();
		checkColumn(columnIndex);		
		return null;
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		checkClosed();
		checkInsideDataArray();
		checkColumn(columnIndex);		
		return null;
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		checkClosed();
		checkInsideDataArray();
		checkColumn(columnIndex);		
		return null;
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		checkClosed();
		checkInsideDataArray();
		checkColumn(columnIndex);		
		return null;
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		checkClosed();
		checkInsideDataArray();
		checkColumn(columnIndex);		
		return null;
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		return getObject(label2index(columnLabel),map);
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		return getRef(label2index(columnLabel));
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		return getBlob(label2index(columnLabel));
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		return getClob(label2index(columnLabel));
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		return getArray(label2index(columnLabel));
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		checkClosed();
		checkInsideDataArray();
		checkColumn(columnIndex);		
		return null;
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		return getDate(label2index(columnLabel));
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		checkClosed();
		checkInsideDataArray();
		checkColumn(columnIndex);		
		return null;
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		return getTime(label2index(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		checkClosed();
		checkInsideDataArray();
		checkColumn(columnIndex);		
		return null;
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		return getTimestamp(label2index(columnLabel));
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		checkClosed();
		checkInsideDataArray();
		checkColumn(columnIndex);		
		return null;
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		return getURL(label2index(columnLabel));
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		updateRef(label2index(columnLabel),x);
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		updateBlob(label2index(columnLabel),x);
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		updateClob(label2index(columnLabel),x);
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		updateArray(label2index(columnLabel),x);
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		checkClosed();
		checkInsideDataArray();
		checkColumn(columnIndex);		
		return null;
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		return getRowId(label2index(columnLabel));
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		updateRowId(label2index(columnLabel),x);
	}

	@Override
	public int getHoldability() throws SQLException {
		return cursorHoldability;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	@Override
	public void updateNString(int columnIndex, String nString) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateNString(String columnLabel, String nString) throws SQLException {
		updateNString(label2index(columnLabel),nString);
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		updateNClob(label2index(columnLabel),nClob);
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		checkClosed();
		checkInsideDataArray();
		checkColumn(columnIndex);		
		return null;
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		return getNClob(label2index(columnLabel));
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		checkClosed();
		checkInsideDataArray();
		checkColumn(columnIndex);		
		return null;
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		return getSQLXML(label2index(columnLabel));
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		updateSQLXML(label2index(columnLabel),xmlObject);
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		checkClosed();
		checkInsideDataArray();
		checkColumn(columnIndex);		
		return null;
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		return getNString(label2index(columnLabel));
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		checkClosed();
		checkInsideDataArray();
		checkColumn(columnIndex);		
		return null;
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		return getNCharacterStream(label2index(columnLabel));
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		updateNCharacterStream(label2index(columnLabel),reader,length);
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		updateAsciiStream(label2index(columnLabel),x,length);
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		updateBinaryStream(label2index(columnLabel),x,length);
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		updateCharacterStream(label2index(columnLabel),reader,length);
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		updateBlob(label2index(columnLabel),inputStream,length);
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		updateClob(label2index(columnLabel),reader,length);
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		updateNClob(label2index(columnLabel),reader,length);
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		updateNCharacterStream(label2index(columnLabel),reader);
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		updateAsciiStream(label2index(columnLabel),x);
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		updateBinaryStream(label2index(columnLabel),x);
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		updateCharacterStream(label2index(columnLabel),reader);
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		updateBlob(label2index(columnLabel),inputStream);
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		updateClob(label2index(columnLabel),reader);
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		checkClosed();
		checkReadOnly();
		checkInsideDataArray();
		checkColumn(columnIndex);		
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		updateNClob(label2index(columnLabel),reader);
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		checkClosed();
		checkInsideDataArray();
		checkColumn(columnIndex);		
		return null;
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		return getObject(label2index(columnLabel),type);
	}

	private int label2index(final String label) throws SQLException {
		if (label == null || label.isEmpty()) {
			throw new IllegalArgumentException("Column label can't be null or empty"); 
		}
		else {
			for (int index = 1; index <= metadata.getColumnCount(); index++) {
				if (metadata.getColumnName(index).equalsIgnoreCase(label)) {
					return index;
				}
			}
			throw new SQLException("Column label ["+label+"] is missing in the result set"); 
		}		
	}
	
	private void checkClosed() throws SQLException {
		if (isClosed) {
			throw new SQLException("Attempt to call method on the closed result set"); 
		}
	}

	private void checkColumn(int columnIndex) throws SQLException {
		if (columnIndex < 1 || columnIndex > metadata.getColumnCount()) {
			throw new IllegalArgumentException("Column index ["+columnIndex+"] out of range 1.."+metadata.getColumnCount()); 
		}
	}

	private void checkReadOnly() throws SQLException {
		if (cursorConcurrency == CONCUR_READ_ONLY) {
			throw new SQLException("Attempt to update read-only result set"); 
		}
	}

	private void checkForwardOnly() throws SQLException {
		if (cursorType == TYPE_FORWARD_ONLY) {
			throw new SQLException("Attempt to move any but next on the forward-only result set"); 
		}
	}

	private void checkInsideDataArray() throws SQLException {
		if (cursorIndex < 0 || cursorIndex >= content.size()) {
			throw new SQLException("Attempt to make operation outside cursor data (possible next() was not called)"); 
		}
	}
}
