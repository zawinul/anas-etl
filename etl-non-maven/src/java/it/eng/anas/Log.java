package it.eng.anas;



public class Log {
	
	public static Log etl = new Log("etl");
	public static Log web = new Log("web");
	public static Log db = new Log("db");
	public static Log fnet = new Log("fnet");
	
	private String logtag;
	
	private Log() {	
	}

	private Log(String logtag) {
		super();
		this.logtag = logtag;
	}
	

	public void log(String x) {
		System.out.println(logtag+":"+x);
	}

	public void warn(String x) {
		System.out.println(logtag+":W:"+x);
	}


	public void log(Exception e) {
		log("Exception:"+e.getMessage());
		e.printStackTrace();
	}

}
