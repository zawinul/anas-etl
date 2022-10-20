package it.eng.anas.threads;

import java.util.List;

public abstract class WorkerFactory  {
	public abstract Worker create(List<Worker> currentJobs);
}
