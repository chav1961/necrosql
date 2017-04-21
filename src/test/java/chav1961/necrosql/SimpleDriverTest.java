package chav1961.necrosql;

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;

import org.junit.Assert;
import org.junit.Test;

public class SimpleDriverTest {
	@Test
	public void spiAndURITest() throws SQLException {
		final Enumeration<Driver> 	list = DriverManager.getDrivers();
		boolean		found = false;
		
		while (list.hasMoreElements()) {
			final Driver	driver = list.nextElement();
			
			if (driver instanceof SimpleDriver) {
				try{DriverManager.getConnection("jdbc:dbf:unknown");
					Assert.fail("Mandartory exception was not detected (non-existent directory for the DBFs)");
				} catch (SQLException exc) {
				}
				
				final File			f = new File("./src/test/resources/chav1961/necrosql");
				final Connection	conn = DriverManager.getConnection("jdbc:dbf:"+f.getAbsolutePath().replace('\\','/'));
				
				Assert.assertNotNull(conn);
				Assert.assertTrue(conn instanceof SimpleConnection);
				conn.close();
				found = true;
			}
		}
		Assert.assertTrue(found);

		try{DriverManager.getConnection("jdbc:unknown");
			Assert.fail("Mandartory exception was not detected (no suitable driver for the given connection string)");
		} catch (SQLException exc) {
		}
		try{DriverManager.getConnection("jdbc");
			Assert.fail("Mandartory exception was not detected (no suitable driver for the given connection string)");
		} catch (SQLException exc) {
		}
	}
}
