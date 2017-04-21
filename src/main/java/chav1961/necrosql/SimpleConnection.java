package chav1961.necrosql;

import java.io.File;
import java.net.URI;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

class SimpleConnection implements Connection {
	private final File			root;
	private final Properties	props;
	
	private SQLWarning			warningChain = null;
	private boolean				autoCommit = true, isClosed = false, isReadOnly;
	
	SimpleConnection(File dirLocated, Properties toCall) throws SQLException {
		this.root = dirLocated;
		this.props = (Properties) toCall.clone();
		
		if (!root.exists() || !root.isDirectory() || !root.canRead()) {
			throw new SQLException("Database location ["+root.getAbsolutePath()+"] is not exists, is not a directory or is not accessible"); 
		}
		else {
			isReadOnly = root.canWrite();
		}
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't have any wrappers for the data");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public Statement createStatement() throws SQLException {
		return createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't support callable statements transactions");
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return sql;
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		this.autoCommit = autoCommit;
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return autoCommit;
	}

	@Override
	public void commit() throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't support transactions");
	}

	@Override
	public void rollback() throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't support transactions");
	}

	@Override
	public void close() throws SQLException {
		if (isClosed) {
			throw new SQLException("Duplicate closure of the connection"); 
		}
		else {
			isClosed = true;
			// TODO Auto-generated method stub
		}
	}

	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return new SimpleMetaData(root,this);
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		if (!readOnly && !root.canWrite()) {
			throw new SQLException("Database location ["+root+"] is not accessible to be read/write!"); 
		}
		else {
			this.isReadOnly = readOnly;
		}
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return isReadOnly;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		throw new SQLFeatureNotSupportedException("Database location catalog ["+catalog+"] can't be changed on-the-fly!"); 
	}

	@Override
	public String getCatalog() throws SQLException {
		return root.getParentFile().getAbsolutePath();
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		if (level != TRANSACTION_NONE) {
			throw new SQLFeatureNotSupportedException("This database doesn't support transactions");
		}
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return TRANSACTION_NONE;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return warningChain;
	}

	@Override
	public void clearWarnings() throws SQLException {
		warningChain = null;
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return createStatement(resultSetType,resultSetConcurrency, getHoldability());
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return prepareStatement(sql,resultSetType,resultSetConcurrency,getHoldability());
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't support callable statements transactions");
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't support callable statements transactions");
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't support callable statements transactions");
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't support transactions");
	}

	@Override
	public int getHoldability() throws SQLException {
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't support transactions");
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't support transactions");
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't support transactions");
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't support transactions");
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return new SimpleStatement(resultSetType,resultSetConcurrency,resultSetHoldability);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return new SimplePreparedStatement(sql,resultSetType,resultSetConcurrency,resultSetHoldability);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't support callable statements transactions");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't support callable statements transactions");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't support callable statements transactions");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException("Thos database doesn't support callable statements transactions");
	}

	@Override
	public Clob createClob() throws SQLException {
		return new SimpleClob();
	}

	@Override
	public Blob createBlob() throws SQLException {
		return new SimpleBlob();
	}

	@Override
	public NClob createNClob() throws SQLException {
		return new SimpleNCLob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		return new SimpleSQLXML();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return true;
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		props.setProperty(name,value);
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		props.putAll(properties);
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return props.getProperty(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return props;
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new SQLFeatureNotSupportedException("Creation of array is not supported for this database!"); 
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new SQLFeatureNotSupportedException("Creation of structure is not supported for this database!"); 
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		throw new SQLFeatureNotSupportedException("Database location schema ["+schema+"] can't be changed on-the-fly!"); 
	}

	@Override
	public String getSchema() throws SQLException {
		return root.getName();
	}

	@Override public void abort(Executor executor) throws SQLException {}
	@Override public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {}
	@Override public int getNetworkTimeout() throws SQLException {return 0;}
}
