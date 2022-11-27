package it.eng.anas.main;


import it.eng.anas.Event;
import it.eng.anas.Global;
import it.eng.anas.Utils;
import it.eng.anas.monitor.WebServer;
import it.eng.anas.threads.ThreadManager;
import it.eng.anas.threads.WorkerFactory;

public class Main {

	public static  void log(String x) { System.out.println(x);}
	static boolean terminated = false;
	WorkerFactory factory;
	ThreadManager manager;
	
	
	public void execute() {
		startWeb();
		startJobs();
		Event.addListener("exit", new Runnable() {
			public void run() {
				terminated = true;
			}
		});
		while(!terminated)
			Utils.longPause();
		spark.Spark.stop();
		manager.killAll(true);
	}
	
	private void startJobs() {
		factory = new WorkerFactory();
		manager = new ThreadManager(factory);
		ThreadManager.mainThreadManager = manager;
		manager.setNumberOfThreads(5);		
	}
	
	private void startWeb() {
		WebServer server = new WebServer();
		server.start();
		server.onKill = ()->terminated = true;		
	}
	
	public static void main(String args[]) throws Exception {
		System.out.println("starting main");
		Global.args = args;
		if (args.length>0)
			Global.configFile = args[0];
		else
			Global.configFile = "./config.json";
		
		System.out.println(Utils.getMapper().writeValueAsString(args));
		new Main().execute();
		
		System.out.println("end of main");

	}
	
}
