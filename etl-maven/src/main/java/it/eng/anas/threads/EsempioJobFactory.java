package it.eng.anas.threads;

import java.util.List;

import it.eng.anas.Utils;
import it.eng.anas.main.MainGetQueues.QDesc;

public class EsempioJobFactory extends JobFactory {
	
	public Job create(List<Job> currentJobs) {
		String tag = Utils.rndString(5);
		
		return new EsempioJob(tag, "estrai-meta", 5);
	}
}
