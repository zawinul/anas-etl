package it.eng.anas.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import it.eng.anas.Utils;
import it.eng.anas.model.Config;

public class DBConnectionFactory {
	public static int nopen=0;
	
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
	
//	private static class Entry {
//		public String label;
//		public DBConnectionFactory factory;
//		public Connection connection;
//	}
	public static HashMap<Connection, String> map = new HashMap<Connection, String>();
	
	
	public Connection getConnection(String label) throws Exception {
		String driverClass = Utils.getConfig().db.driverClass;
		Class.forName(driverClass);
		Connection conn = DriverManager.getConnection(url, user, password);
		String tag = Utils.rndString(6);
		map.put(conn, tag+" "+label);
		nopen++;
		System.out.println("CONNESSIONI APERTE: "+nopen);
		//showMap();
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
			System.out.println("CONNESSIONI APERTE: "+nopen);
			//showMap();

		}
	}
	
	public static String[] getOpenConnectors() {
		return map.values().toArray(new String[0]);
	}
	
	private static void showMap() {
		int i=0;
		for(Entry<Connection, String> entry: map.entrySet()) {
			System.out.println("\tdbconn "+(i++)+" "+entry.getValue());
		}
	}
}
