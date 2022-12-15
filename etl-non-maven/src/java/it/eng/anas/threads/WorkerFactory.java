package it.eng.anas.threads;

import it.eng.anas.etl.AnasEtlWorker;

public class WorkerFactory  {
	
	
	public Worker create(String type, Worker[] currentJobs, String tag) {
		if ("anas-etl".equals(type)) {
			return new AnasEtlWorker(tag);	
		}
		throw new RuntimeException("Worker type "+type+"' not found");
	}

}
