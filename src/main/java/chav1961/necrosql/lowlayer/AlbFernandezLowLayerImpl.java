package chav1961.necrosql.lowlayer;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import chav1961.necrosql.exceptions.NecroSQLLowLayerException;
import chav1961.necrosql.interfaces.LowLayerInterface;

import com.linuxense.javadbf.*;

public class AlbFernandezLowLayerImpl implements LowLayerInterface {
	private static final Map<DBFDataType,DBFDataType>	DECODER = new HashMap<>();
	
	private final File							root;
	private final Map<String,TabeDescRecord>	tableDesc = new HashMap<>();
	
	static {
		DECODER.put(DBFDataType.AUTOINCREMENT,DBFDataType.NUMERIC);
		DECODER.put(DBFDataType.BINARY,DBFDataType.BLOB);
		DECODER.put(DBFDataType.BLOB,DBFDataType.BLOB);
		DECODER.put(DBFDataType.CHARACTER,DBFDataType.CHARACTER);
		DECODER.put(DBFDataType.CURRENCY,DBFDataType.NUMERIC);
		DECODER.put(DBFDataType.DATE,DBFDataType.DATE);
		DECODER.put(DBFDataType.DOUBLE,DBFDataType.NUMERIC);
		DECODER.put(DBFDataType.FLOATING_POINT,DBFDataType.NUMERIC);
		DECODER.put(DBFDataType.GENERAL_OLE,DBFDataType.BLOB);
		DECODER.put(DBFDataType.LOGICAL,DBFDataType.LOGICAL);
		DECODER.put(DBFDataType.LONG,DBFDataType.NUMERIC);
		DECODER.put(DBFDataType.MEMO,DBFDataType.BLOB);
		DECODER.put(DBFDataType.NUMERIC,DBFDataType.NUMERIC);
		DECODER.put(DBFDataType.PICTURE,DBFDataType.BLOB);
		DECODER.put(DBFDataType.TIMESTAMP,DBFDataType.DATE);
		DECODER.put(DBFDataType.TIMESTAMP_DBASE7,DBFDataType.DATE);
		DECODER.put(DBFDataType.UNKNOWN,DBFDataType.BLOB);
		DECODER.put(DBFDataType.VARBINARY,DBFDataType.BLOB);
		DECODER.put(DBFDataType.VARCHAR,DBFDataType.CHARACTER);
	}
	
	public AlbFernandezLowLayerImpl(final File root) {
		this.root = root;
	}
	
	@Override
	public boolean isReadOnly(final String table) throws NecroSQLLowLayerException {
		if (table == null || table.isEmpty()) {
			throw new IllegalArgumentException("Table name can't be null or empty"); 
		}
		else {
			final File	tableFile = new File(root,table+".dbf");
			
			return !(tableFile.exists() && tableFile.isFile() && tableFile.canWrite());
		}
	}

	@Override
	public FieldDesc[] getFieldDesc(final String table) throws NecroSQLLowLayerException {
		if (table == null || table.isEmpty()) {
			throw new IllegalArgumentException("Table name can't be null or empty"); 
		}
		else {
			final File	tableFile = new File(root,table+".dbf");

			if (!tableFile.exists() || !tableFile.isFile() || !tableFile.canRead()) {
				throw new NecroSQLLowLayerException("Table ["+table+"] not exists, is not a file or is not accessible for reading"); 
			}
			else {
				final long		lastModified = tableFile.lastModified();
				
				synchronized(tableDesc) {
					final TabeDescRecord	rec = tableDesc.get(table);
					
					if (rec != null) {
						if (rec.modified < lastModified) {
							tableDesc.remove(table);
						}
						else {
							return rec.description; 
						}
					}
				}
				
				try(final InputStream	is = new FileInputStream(tableFile);
					final DBFReader 	reader = new DBFReader(is)) {
					
					final FieldDesc[]	result = new FieldDesc[reader.getFieldCount()]; 

					for (int index = 0; index < result.length; index++) {
						result[index] = new FieldDescImpl(reader.getField(index));
					}
					synchronized(tableDesc) {
						tableDesc.put(table,new TabeDescRecord(lastModified,result));
					}
					return result;
				} catch (IOException e) {
					throw new NecroSQLLowLayerException("I/O error reading table ["+table+"]: "+e.getMessage()); 
				}
			}
		}
	}

	@Override
	public void setFieldDesc(final String table, final FieldDesc... fields) throws NecroSQLLowLayerException {
		if (table == null || table.isEmpty()) {
			throw new IllegalArgumentException("Table name can't be null or empty"); 
		}
		else {
			final File	tableFile = new File(root,table+".zzz");

			if (!tableFile.isFile() || !tableFile.canWrite() ) {
				throw new NecroSQLLowLayerException("Table ["+table+"] is not a file or is not accessible for writing"); 
			}
			else {
				final DBFField[]		dbfFields = new DBFField[fields.length];
				
				for (int index = 0; index < dbfFields.length; index++) {
					final DBFField		field = new DBFField();
					
					field.setName(fields[index].getName());
					field.setType(DBFDataType.CHARACTER);
					field.setLength(fields[index].getSize());
					field.setDecimalCount(fields[index].getPrecision());
					dbfFields[index] = field;
				}
				
				try(final OutputStream	os = new FileOutputStream(tableFile);
					final DBFWriter 	writer = new DBFWriter(os)) {
					
					writer.setFields(dbfFields);
				} catch (IOException e) {
					throw new NecroSQLLowLayerException("I/O error reading table ["+table+"]: "+e.getMessage()); 
				}
				synchronized(tableDesc) {
					tableDesc.remove(table);
				}
			}
		}
	}
	
	@Override
	public DataArray getData(final String table, boolean forwardOnly) throws NecroSQLLowLayerException {
		if (table == null || table.isEmpty()) {
			throw new IllegalArgumentException("Table name can't be null or empty"); 
		}
		else {
			final File	tableFile = new File(root,table+".dbf");

			if (!tableFile.isFile() || !tableFile.canRead() ) {
				throw new NecroSQLLowLayerException("Table ["+table+"] is not a file or is not accessible for reading"); 
			}
			else {
				if (forwardOnly) {
					return new LazyDataContainer(tableFile);
				}
				else {
					return new DataContainer(tableFile);
				}
			}
		}
	}

	private static class TypeImpl implements SQLType {
		private final DBFDataType	type;
		
		TypeImpl(final DBFDataType type) {
			this.type = type;
		}

		@Override public String getName() {return type.getCharCode()+"";}
		@Override public String getVendor() {return "";}
		@Override public Integer getVendorTypeNumber() {return 1;}

		@Override
		public String toString() {
			return "TypeImpl [type=" + type + "]";
		}
	}
	
	private static class FieldDescImpl implements FieldDesc {
		private final String	name;
		private final SQLType	type;
		private final int		size;
		private final int		precision;
		
		FieldDescImpl(final DBFField desc) {
			this.name = desc.getName();
			this.size = desc.getLength();
			this.precision = desc.getDecimalCount();
			this.type = new TypeImpl(DECODER.containsKey(desc.getType()) ? DECODER.get(desc.getType()) : desc.getType());
		}

		@Override public String getName() {return name;}
		@Override public SQLType getType() {return type;}
		@Override public int getSize() {return size;}
		@Override public int getPrecision() {return precision;}

		@Override
		public String toString() {
			return "FieldDescImpl [name=" + name + ", type=" + type + ", size=" + size + ", precision=" + precision + "]";
		}
	}
	
	private static class TabeDescRecord {
		private final long			modified;
		private final FieldDesc[]	description;
		
		public TabeDescRecord(final long modified, final FieldDesc[] description) {
			this.modified = modified;
			this.description = description;
		}

		@Override
		public String toString() {
			return "TabeDescRecord [modified=" + modified + ", description=" + Arrays.toString(description) + "]";
		}
	}

	private static class LazyDataContainer implements DataArray {
		private final InputStream	content;
		private final DBFReader		rdr;
		private final int			size;
		private int 				position = -1;
		private Object[]			data;
		
		LazyDataContainer(final File table) throws NecroSQLLowLayerException {
			try{content = new FileInputStream(table);
				rdr = new DBFReader(content);
				size = rdr.getRecordCount();
			} catch (IOException e) {
				throw new NecroSQLLowLayerException("I/O error reading table ["+table+"]: "+e.getMessage()); 
			}
		}

		@Override
		public void close() throws IOException {
			rdr.close();
			content.close();
		}

		@Override
		public int position() throws NecroSQLLowLayerException {
			return position;
		}

		@Override
		public int size() throws NecroSQLLowLayerException {
			return size;
		}

		@Override
		public void position(int position) throws NecroSQLLowLayerException {
			if (position != this.position+1) {
				throw new NecroSQLLowLayerException("Forward-only cursor can be moved at the position()+1 only");
			}
			else {
				this.position = position;
				data = rdr.nextRecord();
			}
		}

		@Override
		public Object[] getContent() throws NecroSQLLowLayerException {
			return data;
		}

		@Override
		public void updateContent(Object... content) throws NecroSQLLowLayerException {
			throw new NecroSQLLowLayerException("Forward-only cursor is read-only");
		}

		@Override
		public int insertRecord(Object... content) throws NecroSQLLowLayerException {
			throw new NecroSQLLowLayerException("Forward-only cursor is read-only");
		}

		@Override
		public void deleteRecord() throws NecroSQLLowLayerException {
			throw new NecroSQLLowLayerException("Forward-only cursor is read-only");
		}
		
	}
	
	private static class DataContainer implements DataArray {
		private final File			table;
		private int					size;
		private int 				position = -1;
		private List<Object[]>		data = new ArrayList<>();
		private boolean				changed = false;
		
		DataContainer(final File table) throws NecroSQLLowLayerException {
			this.table = table;
			try(final InputStream	content = new FileInputStream(table);
				final DBFReader		rdr = new DBFReader(content)){
				
				this.size = rdr.getRecordCount();
				for (int index = 0; index < this.size; index++) {
					data.add(rdr.nextRecord());
				}
			} catch (IOException e) {
				throw new NecroSQLLowLayerException("I/O error reading table ["+table+"]: "+e.getMessage()); 
			}
		}

		@Override
		public void close() throws IOException {
			if (changed) {
				try(final OutputStream	content = new FileOutputStream(table);
					final DBFWriter		rdr = new DBFWriter(content)){
					
					for (int index = 0; index < this.size; index++) {
						rdr.addRecord(data.get(index));
					}
				}
			}
		}

		@Override
		public int position() throws NecroSQLLowLayerException {
			return position;
		}

		@Override
		public int size() throws NecroSQLLowLayerException {
			return size;
		}

		@Override
		public void position(final int position) throws NecroSQLLowLayerException {
			this.position = position;
		}

		@Override
		public Object[] getContent() throws NecroSQLLowLayerException {
			return data.get(position());
		}

		@Override
		public void updateContent(final Object... content) throws NecroSQLLowLayerException {
			// TODO Auto-generated method stub
			data.set(position(),content);
			changed = true;
		}

		@Override
		public int insertRecord(Object... content) throws NecroSQLLowLayerException {
			data.add(content);
			this.size = data.size();
			changed = true;
			return this.size - 1;
		}

		@Override
		public void deleteRecord() throws NecroSQLLowLayerException {
			data.remove(position());
			changed = true;
			if (position() >= size()) {
				position(size()-1);
			}
		}
	}
}
