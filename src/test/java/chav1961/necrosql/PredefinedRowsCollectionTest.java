package chav1961.necrosql;

import org.junit.Assert;
import org.junit.Test;

import chav1961.necrosql.interfaces.RowsCollection;

public class PredefinedRowsCollectionTest {

	@Test
	public void emptyCollectionTest() {
		final RowsCollection	prs = new PredefinedRowsCollection(false);
		
		Assert.assertEquals(prs.size(),0);
		Assert.assertEquals(prs.position(),-1);
		try{prs.getRow();
			Assert.fail("Mandatory exception was not detected (position not set)");
		} catch (IllegalStateException exc) {
		}
		try{prs.position(0);
			Assert.fail("Mandatory exception was not detected (position outside the range)");
		} catch (IllegalArgumentException exc) {
		}
	}

	@Test
	public void readOnlyTest() {
		final RowsCollection	prs = new PredefinedRowsCollection(new String[][]{new String[]{"row1.1","row1.2"}},true);
		
		Assert.assertEquals(prs.size(),1);
		Assert.assertEquals(prs.position(),-1);

		try{prs.getRow();
			Assert.fail("Mandatory exception was not detected (position not set)");
		} catch (IllegalStateException exc) {
		}
		
		prs.position(0);
		Assert.assertEquals(prs.position(),0);
		Assert.assertArrayEquals(prs.getRow(),new String[]{"row1.1","row1.2"});
		
		try{prs.updateRow(new String[]{"row1.1","row1.2"});
			Assert.fail("Mandatory exception was not detected (read-only)");
		} catch (IllegalStateException exc) {
		}
		try{prs.deleteRow();
			Assert.fail("Mandatory exception was not detected (read-only)");
		} catch (IllegalStateException exc) {
		}
		try{prs.insertRow();
			Assert.fail("Mandatory exception was not detected (read-only)");
		} catch (IllegalStateException exc) {
		}
		
		try{new PredefinedRowsCollection(null,true);
			Assert.fail("Mandatory exception was not detected (null content)");
		} catch (IllegalArgumentException exc) {
		}
		try{new PredefinedRowsCollection(new String[][]{new String[]{"row1.1","row1.2"},null},true);
			Assert.fail("Mandatory exception was not detected (null row)");
		} catch (IllegalArgumentException exc) {
		}
		try{new PredefinedRowsCollection(new String[][]{new String[]{"row1.1",null}},true);
			Assert.fail("Mandatory exception was not detected (null cell)");
		} catch (IllegalArgumentException exc) {
		}
		try{new PredefinedRowsCollection(new String[][]{new String[]{"row1.1","row1.2"},new String[]{"row2.1"}},true);
			Assert.fail("Mandatory exception was not detected (different row size)");
		} catch (IllegalArgumentException exc) {
		}
		
	}

	@Test
	public void readWriteTest() {
		final RowsCollection	prs = new PredefinedRowsCollection(new String[][]{new String[]{"row1.1","row1.2"}},false);
		
		Assert.assertEquals(prs.size(),1);
		Assert.assertEquals(prs.position(),-1);
		
		prs.position(0);
		Assert.assertArrayEquals(prs.getRow(),new String[]{"row1.1","row1.2"});
		
		try{prs.updateRow(null);
			Assert.fail("Mandatory exception was not detected (null row)");
		} catch (IllegalArgumentException exc) {
		}
		try{prs.updateRow(new String[]{"newRow"});
			Assert.fail("Mandatory exception was not detected (different row size)");
		} catch (IllegalArgumentException exc) {
		}
		try{prs.updateRow(new String[]{"newRow",null});
			Assert.fail("Mandatory exception was not detected (nulls in the row)");
		} catch (IllegalArgumentException exc) {
		}
		
		prs.updateRow(new String[]{"newRow1.1","newRow1.2"});
		Assert.assertArrayEquals(prs.getRow(),new String[]{"newRow1.1","newRow1.2"});
		
		Assert.assertEquals(prs.insertRow(),1);		
		Assert.assertEquals(prs.size(),2);
		Assert.assertTrue(prs.getRow() instanceof String[]);
		
		prs.deleteRow();
		Assert.assertEquals(prs.size(),1);
		Assert.assertEquals(prs.position(),0);
		Assert.assertFalse(prs.getRow() instanceof String[]);

		prs.deleteRow();
		Assert.assertEquals(prs.size(),0);
		Assert.assertEquals(prs.position(),-1);
	}
}
