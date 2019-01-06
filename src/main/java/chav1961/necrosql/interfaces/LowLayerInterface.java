package chav1961.necrosql.interfaces;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLType;

import chav1961.necrosql.exceptions.NecroSQLLowLayerException;

public interface LowLayerInterface {
	public interface FieldDesc {
		String getName();
		SQLType getType();
		int getSize();
		int getPrecision();
	}

	public interface DataArray extends Closeable {
		int position() throws NecroSQLLowLayerException;
		int size() throws NecroSQLLowLayerException;
		void position(int position) throws NecroSQLLowLayerException;
		Object[] getContent() throws NecroSQLLowLayerException;
		void updateContent(Object... content) throws NecroSQLLowLayerException;
		int insertRecord(Object... content) throws NecroSQLLowLayerException;
		void deleteRecord() throws NecroSQLLowLayerException;
	}
	
	boolean isReadOnly(String table) throws NecroSQLLowLayerException;
	FieldDesc[] getFieldDesc(String table) throws NecroSQLLowLayerException;
	void setFieldDesc(String table, FieldDesc... fields) throws NecroSQLLowLayerException;
	DataArray getData(String table, boolean forwardOnly) throws NecroSQLLowLayerException;
}
