package it.eng.anas.model;

import java.util.HashMap;

public class Config extends Model {
//	public String processingQueue ="";
//	public String metadataQueue = "";
//	public String contentQueue = "";

	public String extractCondition = "1=1";
	
	public String outputBasePath = "";
	public int webServerPort = 5150;
	public FilenetConfig filenet = new FilenetConfig();
	public FilenetDBConfig filenetdb = new FilenetDBConfig();
	public DbConfig db = new DbConfig();
	public int shortPause[] = {100, 200};
	public int longPause[] = {10000, 30000};
	public int nMaxRetry = 10;
	public boolean simulazioneErrori = false;
	public HashMap<String, Integer> schedule = new HashMap<String, Integer>();
	public boolean websocketEnabled= false;
	
	public String idProgetti;
	public String idLavori;
	public String idArchivi;

	
	public static class FilenetConfig  extends Model {
		public String userid = "";
		public String password = "";
		public String uri = "";
		public String objectstore = "";
		public String stanza = "";
	}

	public static class FilenetDBConfig  extends Model {
		public String user = "";
		public String password = "";
		public String url = "";
		public String schema= "";
		public String driverclass = "";
	}

	
	public static class DbConfig  extends Model {
		public String url = "";
		public String username  = "";
		public String password  = "";
		public String driverClass = "";
	}
}
