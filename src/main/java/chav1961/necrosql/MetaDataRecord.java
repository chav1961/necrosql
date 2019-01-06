package chav1961.necrosql;

import java.sql.SQLType;

import chav1961.necrosql.interfaces.LowLayerInterface.FieldDesc;

class MetaDataRecord implements FieldDesc {
	String		name;
	SQLType		type;
	int			size;
	int			precision;

	MetaDataRecord(final String name, final SQLType type, final int size) {
		this(name,type,size,0);
	}

	MetaDataRecord(final String name, final SQLType type, final int size, final int precision) {
		this.name = name;
		this.type = type;
		this.size = size;
		this.precision = precision;
	}

	@Override public String getName() {return name;}
	@Override public SQLType getType() {return type;}
	@Override public int getSize() {return size;}
	@Override public int getPrecision() {return precision;}

	@Override
	public String toString() {
		return "MetaDataRecord [name=" + name + ", type=" + type + ", size=" + size + ", precision=" + precision + "]";
	}
}