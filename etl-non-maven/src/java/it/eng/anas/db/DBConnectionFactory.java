package it.eng.anas.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;

import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.model.Config;

public class DBConnectionFactory {
	public static int nopen=0;
	public static int nerrors=0;
	public DBConnectionFactory(String url, String user, String password) {
		this.url = url;
		this.user = user;
		this.password = password;
	}

	public static DBConnectionFactory defaultFactory;
	static {
		Config cfg = Utils.getConfig();
		defaultFactory = new DBConnectionFactory(
				cfg.db.url,
				cfg.db.username,
				cfg.db.password);
	}

	private String url;
	private String user;
	private String password;
	
	public static HashMap<Connection, String> map = new HashMap<Connection, String>();
	
	public Connection getConnection(String label) throws Exception {
		String driverClass = Utils.getConfig().db.driverClass;
		Class.forName(driverClass);
		Connection conn = DriverManager.getConnection(url, user, password);
		map.put(conn, label);
		nopen++;
		//Log.log("+ CONNESSIONI APERTE: "+nopen);
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
			nerrors++;
			Log.log("+ERRORE CHIUSURA CONNESSIONE "+nerrors);
			Log.log(e);
		}
		finally {
			nopen--;
			//Log.log("-CONNESSIONI APERTE: "+nopen);
			//showMap();
		}
	}
	
	public static String[] getOpenConnectors() {
		return map.values().toArray(new String[0]);
	}

}
