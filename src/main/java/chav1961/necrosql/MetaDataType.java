package chav1961.necrosql;

import java.sql.SQLType;

class MetaDataType implements SQLType {
	static final char		CHAR_LETTER = 'C';
	static final char		NUMBER_LETTER = 'N';
	static final char		DATE_LETTER = 'D';
	static final char		LOGICAL_LETTER = 'L';
	static final char		CLOB_LETTER = 'M';
	
	static final SQLType	CHAR_TYPE = new MetaDataType("CHARACTER",CHAR_LETTER); 
	static final SQLType	NUMBER_TYPE = new MetaDataType("NUMBER",NUMBER_LETTER); 
	static final SQLType	DATE_TYPE = new MetaDataType("DATE",DATE_LETTER); 
	static final SQLType	LOGICAL_TYPE = new MetaDataType("BOOLEAN",LOGICAL_LETTER); 
	static final SQLType	CLOB_TYPE = new MetaDataType("CLOB",CLOB_LETTER); 
	
	private final String	name;
	private final Integer	id;
	
	private MetaDataType(final String name, final char id) {
		this.name = name;
		this.id = Integer.valueOf(id);
	}

	@Override public String getName() {return name;}
	@Override public String getVendor() {return "necroSQL";}
	@Override public Integer getVendorTypeNumber() {return id;}

	@Override
	public String toString() {
		return "MetaDataType [name=" + name + ", id=" + ((char)id.intValue()) + "]";
	}		
}