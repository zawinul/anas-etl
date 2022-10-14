package it.eng.anas.threads;

import java.util.List;
import it.eng.anas.main.MainGetQueues.QDesc;

public abstract class JobFactory  {
	public abstract Job create(List<Job> currentJobs);
}
