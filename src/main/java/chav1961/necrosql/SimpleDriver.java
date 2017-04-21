package chav1961.necrosql;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * <p>This class supports SQL queries to the local DBF files.</p>
 * <p>To use this driver in the JDBC environment, type:</p>
 * <p><code>Connection conn = DriverManager.getConnection("jdbc:dbf:<path_to_the_directory_with_dbf_files>");</p>   
 * @author Alexander Chernomyrdin aka chav1961
 * @since 0.0.1
 */

public class SimpleDriver implements Driver {
	static final String				DRIVER_PRODUCT = "product name";
	static final String				DRIVER_NAME = "driver name";
	static final int				DRIVER_MAJ_VER = 0;
	static final int				DRIVER_MIN_VER = 1;
	static final String				PROP_USER = "user";
	
	private static final Logger		logger = Logger.getLogger(SimpleDriver.class.getName());
	private static final String		PROP_PASSWD = "password";
	private static final String		PROP_ENCODING = "encoding";
	
	static {
		try{DriverManager.registerDriver(new SimpleDriver());
		} catch (SQLException e) {
			System.err.println("Registration of the SQL driver failed: "+e.getMessage());
		}
	}

	@Override
	public Connection connect(final String url, final Properties info) throws SQLException {
		if (acceptsURL(url)) {
			final String		path = URI.create(URI.create(url).getSchemeSpecificPart()).getSchemeSpecificPart();
			final File			file = new File(path.contains("?") ? path.substring(0,path.indexOf('?')) : path);
			final Properties	toCall = extractProps(path.contains("?") ? path.substring(path.indexOf('?')+1) : "");
			
			toCall.putAll(info);
			return connect(file,toCall);
		}
		else {
			return null;
		}
	}

	@Override
	public boolean acceptsURL(final String url) throws SQLException {
		if (url == null || url.isEmpty()) {
			return false;
		}
		else {
			final URI		uri = URI.create(url);
			
			if ("jdbc".equals(uri.getScheme())) {
				final URI	uriTail = URI.create(uri.getSchemeSpecificPart());
				
				if ("dbf".equals(uriTail.getScheme())) {
					return true;
				}
				else {
					return false;
				}
			}
			else {
				return false;
			}
		}
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) throws SQLException {
		if (acceptsURL(url)) {
			final String		path = URI.create(URI.create(url).getSchemeSpecificPart()).getSchemeSpecificPart();
			final Properties	toCall = extractProps(path.contains("?") ? path.substring(path.indexOf('?')+1) : "");

			final List<DriverPropertyInfo>	result = new ArrayList<>();
			
			for (Entry<Object, Object> item : toCall.entrySet()) {
				result.add(new DriverPropertyInfo(item.getKey().toString(),item.getValue().toString()));
			}
			return result.toArray(new DriverPropertyInfo[result.size()]);
		}
		else {
			return new DriverPropertyInfo[0];
		}
	}

	@Override public int getMajorVersion() {return DRIVER_MAJ_VER;}
	@Override public int getMinorVersion() {return DRIVER_MIN_VER;}
	@Override public boolean jdbcCompliant() {return false;}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return logger.getParent();
	}
	
	private Properties extractProps(final String query) {
		final Properties	props = new Properties();
		
		try(final Reader	rdr = new StringReader(query.replace('&','\n'))) {
			props.load(rdr);
		} catch (IOException e) {
		}
		return props;
	}

	private Connection connect(final File dirLocated, final Properties toCall) throws SQLException {
		return new SimpleConnection(dirLocated,toCall);
	}
}
