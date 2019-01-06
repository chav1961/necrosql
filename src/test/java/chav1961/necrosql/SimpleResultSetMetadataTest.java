package chav1961.necrosql;


import java.io.File;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.junit.Assert;
import org.junit.Test;

public class SimpleResultSetMetadataTest {
	@Test
	public void metaDataRecordTest() {
		final MetaDataRecord	mdr = new MetaDataRecord("NAME",MetaDataType.CHAR_TYPE,10,2);
		
		Assert.assertEquals(mdr.getName(),"NAME");
		Assert.assertEquals(mdr.getType(),MetaDataType.CHAR_TYPE);
		Assert.assertEquals(mdr.getSize(),10);
		Assert.assertEquals(mdr.getPrecision(),2);
	}

	@Test
	public void resultSetMetaDataTest() throws SQLException {
		final SimpleResultSetMetadata	md = new SimpleResultSetMetadata(new File("./"),"myTable",false
														,new MetaDataRecord("FIELD1",MetaDataType.CHAR_TYPE,10)
														,new MetaDataRecord("FIELD2",MetaDataType.NUMBER_TYPE,10,2)
														,new MetaDataRecord("FIELD3",MetaDataType.DATE_TYPE,8)
														);
		
		Assert.assertEquals(md.getColumnCount(),3);
		try{md.getColumnClassName(0);
			Assert.fail("Mandatory exception was not detected (column number out of range)");
		} catch (IllegalArgumentException exc) {
		}
		try{md.getColumnClassName(4);
			Assert.fail("Mandatory exception was not detected (column number out of range)");
		} catch (IllegalArgumentException exc) {
		}
		
		Assert.assertNotNull(md.getCatalogName(1));
		Assert.assertNull(md.getSchemaName(1));
		Assert.assertEquals(md.getTableName(1),"myTable");
		
		try{md.unwrap(String.class);
			Assert.fail("Mandatory exception was not detected (feature not supported)");
		} catch (SQLFeatureNotSupportedException exc) {
		}
		Assert.assertFalse(md.isWrapperFor(String.class));

		Assert.assertFalse(md.isAutoIncrement(1));
		Assert.assertFalse(md.isCaseSensitive(1));
		Assert.assertFalse(md.isSearchable(1));
		
		Assert.assertFalse(md.isCurrency(1));
		Assert.assertTrue(md.isCurrency(2));

		Assert.assertEquals(md.isNullable(1),0);

		Assert.assertFalse(md.isSigned(1));
		Assert.assertTrue(md.isSigned(2));

		Assert.assertEquals(md.getColumnLabel(1),"FIELD1");
		Assert.assertEquals(md.getColumnName(1),"FIELD1");
		Assert.assertEquals(md.getColumnDisplaySize(1),10);
		Assert.assertEquals(md.getPrecision(1),0);
		Assert.assertEquals(md.getPrecision(2),2);
		Assert.assertEquals(md.getScale(1),0);

		Assert.assertEquals(md.getColumnType(1),MetaDataType.CHAR_LETTER);
		Assert.assertEquals(md.getColumnTypeName(1),"CHARACTER");

		Assert.assertFalse(md.isReadOnly(1));
		Assert.assertTrue(md.isWritable(1));
		Assert.assertTrue(md.isDefinitelyWritable(1));

		Assert.assertEquals(md.getColumnClassName(1),String.class.getName());
	}
}
