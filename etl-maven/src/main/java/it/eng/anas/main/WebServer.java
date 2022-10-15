package it.eng.anas.main;

import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.model.Config;
import it.eng.anas.threads.ThreadManager;

public class WebServer {

	public WebServer(ThreadManager manager) {
		super();
		this.manager = manager;
	}

	public ThreadManager manager;
	public Runnable onKill;
	public void start() {
		Config c = Utils.getConfig();
		spark.Spark.port(c.webServerPort);
		spark.Spark.get("/hello", (req, res) -> {
			Log.web.log(req.url().toString());
			
			return "Hello World "+Math.random()+ " "+req.url();
		});

		spark.Spark.get("/setn/:n", (req, res) -> {
			Log.web.log(req.url().toString());
			int n = Integer.parseInt(req.params("n"));
			manager.setNumberOfThreads(n);
			return "Hello World n="+n+ " "+req.url();
		});

		spark.Spark.get("/kill-all", (req, res) -> {
			Log.web.log(req.url().toString());
			Log.web.log("before kill all (wait=true)");
			manager.killAll(true);
			Log.web.log("after kill all");
			if (onKill!=null)
				onKill.run();
			return "kill-all after wait";
		});

	}

	
}
