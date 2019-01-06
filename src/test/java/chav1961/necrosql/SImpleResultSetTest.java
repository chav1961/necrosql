package chav1961.necrosql;

import java.io.File;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;

public class SImpleResultSetTest {

	@Test
	public void emptyResultSetTest() throws SQLException {
		final ResultSetMetaData	md = new SimpleResultSetMetadata(new File("./"),"myTable",false
												,new MetaDataRecord("FIELD1",MetaDataType.CHAR_TYPE,10)
												,new MetaDataRecord("FIELD2",MetaDataType.NUMBER_TYPE,10,2)
												,new MetaDataRecord("FIELD3",MetaDataType.DATE_TYPE,8)
												);
		final ResultSet			rs = new SimpleResultSet(md);
		
		Assert.assertEquals(rs.getMetaData(),md);
		Assert.assertFalse(rs.next());
		
		try{rs.getString(100);
			Assert.fail("Mandatory exception was not detected (column inde out of range)");
		} catch (IllegalArgumentException exc) {
		}
		try{rs.getString("UNKNOWN");			
			Assert.fail("Mandatory exception was not detected (unknown column name)");
		} catch (SQLException exc) {
		}
		try{rs.getString(1);			
			Assert.fail("Mandatory exception was not detected (no data in the cursor)");
		} catch (SQLException exc) {
		}
		
		rs.close();
		try{rs.close();			
			Assert.fail("Mandatory exception was not detected (dupicate closure)");
		} catch (SQLException exc) {
		}
		
	}
}
