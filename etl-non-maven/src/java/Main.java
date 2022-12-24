


import it.eng.anas.Event;
import it.eng.anas.Global;
import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.etl.Background;
import it.eng.anas.monitor.WebServer;
import it.eng.anas.threads.ThreadManager;

public class Main {

	public static  void log(String x) { System.out.println(x);}
	static boolean terminated = false;
	ThreadManager manager;
	
	
	public void execute() {
		startWeb();
		startJobs();
		Event.addListener("exit", new Runnable() {
			public void run() {
				terminated = true;
			}
		});
		Background.start();
		while(!terminated)
			Utils.longPause();
		spark.Spark.stop();
		manager.killAll(true);
		Background.join();
	}
	
	private void startJobs() {
		manager = new ThreadManager();
		ThreadManager.mainThreadManager = manager;
		manager.setNumberOfThreads(1);		
	}
	
	private void startWeb() {
		WebServer server = new WebServer();
		server.start();
		server.onKill = ()->terminated = true;	
		Log.log("Web started on port "+Utils.getConfig().webServerPort);
	}
	
	public static void main(String args[]) throws Exception {
		System.out.println("starting main");
		Global.args = args;
		Global.configFile = "./config.json";
		for(String arg: args) {
			if (arg.equals("debug"))
				Global.debug = true;
			else if (arg.endsWith(".json"))
				Global.configFile = arg;
		}
		
		System.out.println(Utils.getMapper().writeValueAsString(args));
		new Main().execute();
		
		System.out.println("end of main");

	}
	
}
