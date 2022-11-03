package it.eng.anas.main;


import it.eng.anas.Event;
import it.eng.anas.Utils;
import it.eng.anas.etl.AnasEtlWorkerFactory;
import it.eng.anas.monitor.WebServer;
import it.eng.anas.threads.ThreadManager;
import it.eng.anas.threads.WorkerFactory;

public class Main {

	public static  void log(String x) { System.out.println(x);}
	static boolean terminated = false;
	WorkerFactory factory;
	ThreadManager manager;
	
	
	public void execute() {
		startJobs();
		startWeb();
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
		factory = new AnasEtlWorkerFactory();
		manager = new ThreadManager(factory);
		ThreadManager.mainThreadManager = manager;
		manager.setNumberOfThreads(-1);		
	}
	
	private void startWeb() {
		WebServer server = new WebServer();
		server.start();
		server.onKill = ()->terminated = true;		
	}
	
	public static void main(String args[]) {
		System.out.println("starting main");
		new Main().execute();
		
		System.out.println("end of main");

	}
	
}
