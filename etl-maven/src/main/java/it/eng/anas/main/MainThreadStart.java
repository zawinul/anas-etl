package it.eng.anas.main;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Utils;
import it.eng.anas.WebMonitor;
import it.eng.anas.main.MainGetQueues.QDesc;
import it.eng.anas.threads.DBConsumeThreadFactory;
import it.eng.anas.threads.EsempioJobFactory;
import it.eng.anas.threads.JobFactory;
import it.eng.anas.threads.QueueConsumeThreadFactory;
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
		startWeb();
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
	
	private void startWeb() {
		spark.Spark.port(5151);
		spark.Spark.get("/hello", (req, res) -> {
			log(req.url().toString());
			
			return "Hello World "+Math.random()+ " "+req.url();
		});

		spark.Spark.get("/setn/:n", (req, res) -> {
			log(req.url().toString());
			int n = Integer.parseInt(req.params("n"));
			manager.setNumberOfThreads(n);
			return "Hello World n="+n+ " "+req.url();
		});

		spark.Spark.get("/kill-all", (req, res) -> {
			log(req.url().toString());
			log("before kill all (wait=true)");
			manager.killAll(true);
			log("after kill all");
			terminated = true;
			return "kill-all after wait";
		});

		spark.Spark.get("/queues", (req, res) -> {
			ObjectMapper mapper = Utils.getMapper();
			List<QDesc> queues = new WebMonitor().getQueues();
			return mapper.writeValueAsString(queues);
		});
		
	}

	
}
