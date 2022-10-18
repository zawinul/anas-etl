package it.eng.anas.etl;

import java.util.List;

import it.eng.anas.Utils;
import it.eng.anas.threads.Job;
import it.eng.anas.threads.JobFactory;

public class DBConsumeThreadFactory extends JobFactory {
	
	public Job create(List<Job> currentJobs) {
		String tag = Utils.rndString(5);
		String queue = "esempio1";
		int threadPriority = 5;
		return new AnasEtlJob(tag, queue, threadPriority);	
	}
}
