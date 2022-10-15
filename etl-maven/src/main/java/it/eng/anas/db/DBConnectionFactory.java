package it.eng.anas.db;

import java.sql.Connection;
import java.sql.DriverManager;

public class DBConnectionFactory {
	
	public DBConnectionFactory(String url, String user, String password) {
		super();
		this.url = url;
		this.user = user;
		this.password = password;
	}

	public static DBConnectionFactory defaultFactory = new DBConnectionFactory(
			"jdbc:mysql://localhost:3306/anas-etl-prod", 
			"anas-etl-prod", 
			"anas-etl-prod");

	private String url;
	private String user;
	private String password;

	
	public Connection getConnection() throws Exception {
		@SuppressWarnings("unused")
		Class c = Class.forName("com.mysql.cj.jdbc.Driver");
		Connection conn = DriverManager.getConnection(url, user, password);
		return conn;
	}
}
