package it.eng.anas.model;

import java.util.HashMap;

public class Config extends Model {
	public String outputBasePath = "";
	public int webServerPort = 5150;
	public FilenetConfig filenet = new FilenetConfig();
	public DbConfig db = new DbConfig();
	public int shortPause[] = {100, 200};
	public int longPause[] = {10000, 30000};
	public int nMaxRetry = 10;
	
	public HashMap<String, Integer> schedule = new HashMap<String, Integer>();

	public static class FilenetConfig  extends Model {
		public String userid = "";
		public String password = "";
		public String uri = "";
		public String stanza = "";
	}

	
	public static class DbConfig  extends Model {
		public String url = "";
		public String username  = "";
		public String password  = "";
	}
}
