package it.eng.anas.db;

import java.sql.Connection;
import java.sql.DriverManager;

import it.eng.anas.Utils;
import it.eng.anas.model.Config;

public class DBConnectionFactory {
	
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

	
	public Connection getConnection() throws Exception {
		String driverClass = Utils.getConfig().db.driverClass;
		Class.forName(driverClass);
		Connection conn = DriverManager.getConnection(url, user, password);
		return conn;
	}
}
