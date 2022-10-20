package it.eng.anas.monitor;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import it.eng.anas.Event;
import it.eng.anas.Log;
import it.eng.anas.UTF8;
import it.eng.anas.Utils;
import it.eng.anas.model.Config;
import it.eng.anas.threads.ThreadManager;

public class WebServer {

	public WebServer() {
	}

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
			ThreadManager.mainThreadManager.setNumberOfThreads(n);
			return "Hello World n="+n+ " "+req.url();
		});

		spark.Spark.get("/report", (req, res) -> {
			return new StatusReport().getReport();
		});


		spark.Spark.get("/jobs", (req, res) -> {
			return new StatusReport().getJobs();
		});

		spark.Spark.get("/start/:class", (req, res) -> {
			String docClass = req.params("class");
			Event.emit("start-simulation", docClass);
			return "ok";
		});


		spark.Spark.get("/web/:file", (req, res) -> {
			String file = req.params("file");
			if (file==null||file.trim().equals(""))
				file="index.html";
			InputStream s = this.getClass().getClassLoader().getResourceAsStream("web/"+file);
			String ret = IOUtils.toString(s, UTF8.charset);
			return ret;
		});

		spark.Spark.get("/exit", (req, res) -> {
			Event.emit("exit");
			return "exit launched";
		});

	}

	
}
