package it.eng.anas.monitor;


import java.io.FileWriter;
import java.util.Map;

import it.eng.anas.Event;
import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.etl.AnasEtlJob;
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

		spark.Spark.get("/threads", (req, res) -> {
			try {
				Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
				FileWriter w = new FileWriter("./thread-dump.txt");
				for(Thread t:map.keySet()) {
					w.write(t+"\n");
					StackTraceElement[] tr = map.get(t);
					for(StackTraceElement element:tr) {
						w.write("\t"+element.toString()+"\n");						
					}
					Log.etl.log(t+"\n");
				}
				w.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			return "ok";
		});
		spark.Spark.get("/report", (req, res) -> {
			return new StatusReport().getReport();
		});

		spark.Spark.post("/insertJob", (req, res) -> {
			DbJobManager<AnasEtlJob> manager = new DbJobManager("insertJob", AnasEtlJob.class);
			String json = req.body();
			AnasEtlJob input = Utils.getMapper().readValue(json, AnasEtlJob.class);
			AnasEtlJob job = manager.insertNew(input);
			manager.close();
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



	}

	private String n(String x) {
		if (x==null || x.trim().equals(""))
			return null;
		else
			return x;
	}
	
}
