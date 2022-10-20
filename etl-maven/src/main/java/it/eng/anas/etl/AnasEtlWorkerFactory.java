package it.eng.anas.etl;

import java.util.List;

import it.eng.anas.Utils;
import it.eng.anas.threads.Worker;
import it.eng.anas.threads.WorkerFactory;

public class AnasEtlWorkerFactory extends WorkerFactory {
	
	public Worker create(List<Worker> currentJobs) {
		String tag = Utils.rndString(5);
		String queue = "anas-etl";
		int threadPriority = 5;
		return new AnasEtlWorker(tag, queue, threadPriority);	
	}
}
