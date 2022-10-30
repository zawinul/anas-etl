package it.eng.anas.monitor;


import it.eng.anas.Event;
import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.model.Config;
import it.eng.anas.model.DBJob;
import it.eng.anas.threads.ThreadManager;

public class WebServer {

	public WebServer() {
	}

	public Runnable onKill;
	public void start() {
		Config c = Utils.getConfig();
		spark.Spark.port(c.webServerPort);
		spark.Spark.staticFileLocation("/web");
		spark.Spark.get("/hello", (req, res) -> {
			Log.web.log(req.url().toString());
			
			return "Hello World "+Math.random()+ " "+req.url();
		});

		spark.Spark.get("/setn/:n", (req, res) -> {
			Log.web.log(req.url().toString());
			int n = Integer.parseInt(req.params("n"));
			ThreadManager.mainThreadManager.setNumberOfThreads(n);
			return "Hello World n="+n+ " "+req.url();
		});

		spark.Spark.get("/report", (req, res) -> {
			return new StatusReport().getReport();
		});

		spark.Spark.get("/insertJob", (req, res) -> {
			//insertJob?queue=anas-etl&priority=1000&operation=prova&par1=&par2=&par3=&parentJob=&extra=
			
//			insertNew(String queue, int priority,  
//					String operation, String par1, String par2, String par3, int parentJob, String extra)
			DbJobManager manager = new DbJobManager();
			String queue = req.queryParams("queue");
			int priority = Integer.parseInt(req.queryParams("priority"));
			String operation  = req.queryParams("operation");		
			String par1  = n(req.queryParams("par1"));		
			String par2  = n(req.queryParams("par2"));		
			String par3  = n(req.queryParams("par3"));		
			String sParentJob  = n(req.queryParams("parentJob"));	
			int parentJob = sParentJob==null ? -1 : Integer.parseInt(sParentJob);
			String extra  = n(req.queryParams("extra"));		
			DBJob job = manager.insertNew(queue,priority,operation,par1, par2, par3, parentJob, extra);
			return Utils.getMapper().writeValueAsString(job);
		});


		spark.Spark.get("/jobs", (req, res) -> {
			return new StatusReport().getJobs();
		});

		spark.Spark.get("/start/:class", (req, res) -> {
			String docClass = req.params("class");
			Event.emit("start-simulation", docClass);
			return "ok";
		});

		spark.Spark.get("/exit", (req, res) -> {
			Event.emit("exit");
			return "exit launched";
		});


//		spark.Spark.get("/favicon.ico", (req, res) -> {
//		});

	}

	private String n(String x) {
		if (x==null || x.trim().equals(""))
			return null;
		else
			return x;
	}
	
}
