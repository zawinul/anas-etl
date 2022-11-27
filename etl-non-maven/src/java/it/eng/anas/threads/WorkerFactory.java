package it.eng.anas.threads;

import it.eng.anas.Utils;
import it.eng.anas.etl.AnasEtlWorker;

public class WorkerFactory  {
	
	
	public Worker create(String type, Worker[] currentJobs, String tag) {
		if ("anas-etl".equals(type)) {
			String queue = Utils.getConfig().queue;
			return new AnasEtlWorker(tag, queue);	
		}
		throw new RuntimeException("Worker type "+type+"' not found");
	}

}
