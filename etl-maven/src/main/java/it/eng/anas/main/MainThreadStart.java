package it.eng.anas.main;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Utils;
import it.eng.anas.threads.DBConsumeThreadFactory;
import it.eng.anas.threads.EsempioJobFactory;
import it.eng.anas.threads.JobFactory;
import it.eng.anas.threads.ThreadManager;

public class MainThreadStart {

	public static  void log(String x) { System.out.println(x);}
	static boolean terminated = false;
	JobFactory factory;
	ThreadManager manager;
	
	public static void main(String[] args) throws Exception {
		MainThreadStart instance = new MainThreadStart();
		instance.execute();
		log("end of main");
	}
	
	public void execute() {
		WebServer server = new WebServer(manager);
		server.start();
		server.onKill = ()->terminated = true;
		startJobs();
		while(!terminated)
			Utils.sleep(1000);
		
		spark.Spark.stop();
	}
	
	private void startJobs() {
		factory = new DBConsumeThreadFactory();
		manager = new ThreadManager(factory);
		manager.setNumberOfThreads(10);		
	}
	
	
}
