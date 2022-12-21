package it.eng.anas.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;

import it.eng.anas.Utils;
import it.eng.anas.model.Config;

public class FilenetDBConnectionFactory {
	public static int nopen=0;
	
	public FilenetDBConnectionFactory(String url, String user, String password, String schema) {
		this.url = url;
		this.user = user;
		this.password = password;
	}

	public static FilenetDBConnectionFactory defaultFactory;
	static {
		Config cfg = Utils.getConfig();
		defaultFactory = new FilenetDBConnectionFactory(
			cfg.filenetdb.url,
			cfg.filenetdb.user,
			cfg.filenetdb.password,
			cfg.filenetdb.schema
		);
	}

	private String url;
	private String user;
	private String password;
	
	public static HashMap<Connection, String> map = new HashMap<Connection, String>();
	
	public Connection getConnection(String label) throws Exception {
		if (!Utils.getConfig().directFilenetDbAccess)
			throw new Exception("non è possibile accedere al db di filenet");
		String driverClass = Utils.getConfig().filenetdb.driverclass;
		Class.forName(driverClass);
		Connection conn = DriverManager.getConnection(url, user, password);
		map.put(conn, label);
		nopen++;
		//Log.etl.log("CONNESSIONI APERTE: "+nopen);
		return conn;
	}
	
	public static void close(Connection c) {
		if (c==null)
			return;

		if (map.containsKey(c))
			map.remove(c);

		try {
			if(c.isClosed())
				return;
		}catch(Exception e) {
			e.printStackTrace();
			return;
		}
		
		try {
			c.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			nopen--;
			//Log.etl.log("CONNESSIONI APERTE: "+nopen);
			//showMap();
		}
	}
	
	public static String[] getOpenConnectors() {
		return map.values().toArray(new String[0]);
	}

}
