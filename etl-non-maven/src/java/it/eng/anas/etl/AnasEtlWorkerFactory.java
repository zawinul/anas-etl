package it.eng.anas.etl;

import java.util.List;

//import it.eng.anas.Utils;
import it.eng.anas.threads.Worker;
import it.eng.anas.threads.WorkerFactory;

public class AnasEtlWorkerFactory extends WorkerFactory {
	
	private static int prog = 0;
	
	public Worker create(List<Worker> currentJobs) {
		prog++;
		String tag = "WRK-"+(""+(100000+prog)).substring(1);
		String queue = "anas-etl";
		int threadPriority = 5;
		return new AnasEtlWorker(tag, queue, threadPriority);	
	}
}
