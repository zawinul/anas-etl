package it.eng.anas.main;

import java.util.function.Consumer;

import it.eng.anas.Event;
import it.eng.anas.Utils;
import it.eng.anas.etl.AnasEtlJobProcessor;
import it.eng.anas.etl.AnasEtlWorkerFactory;
import it.eng.anas.monitor.WebServer;
import it.eng.anas.threads.WorkerFactory;
import it.eng.anas.threads.ThreadManager;

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
		Event.addListener("start-simulation", new Consumer<Object>() {
			public void accept(Object t) {
				AnasEtlJobProcessor.startSimulation((String) t);
			}
		});

		new Main().execute();
	}
}
