package it.eng.anas.threads;

import java.util.List;

public abstract class JobFactory  {
	public abstract Job create(List<Job> currentJobs);
}
