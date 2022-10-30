package it.eng.anas;



public class Log {
	
	public static Log main = new Log("main");
	public static Log web = new Log("web");
	public static Log db = new Log("db");
	public static Log config = new Log("cfg");
	public static Log queue = new Log("queue");
	public static Log fnet = new Log("fnet");
	
	private String logtag;
	
	private Log() {	
	}

	private Log(String logtag) {
		super();
		this.logtag = logtag;
	}
	

	public void log(String x) {
		System.out.println(logtag+":i:"+x);
	}

	public void warn(String x) {
		System.out.println(logtag+":W:"+x);
	}


	public void log(Exception e) {
		System.out.println(logtag+":Exception:"+e.getMessage());
		e.printStackTrace();
	}

}
