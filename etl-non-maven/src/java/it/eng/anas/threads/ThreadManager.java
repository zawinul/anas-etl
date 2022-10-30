package it.eng.anas.threads;

import java.util.ArrayList;
import java.util.List;

import it.eng.anas.Event;
import it.eng.anas.ScheduleHelper;

public class ThreadManager {
	public int NMAXTHREADS = 200;
	public List<Worker> threads = new ArrayList<Worker>();
	public WorkerFactory factory;
	public int forcedNumberOfThreads = 0;
	
	public static ThreadManager mainThreadManager;
	
	public ThreadManager(WorkerFactory factory) {
		this.factory = factory;
		Event.addListener("exit", new Runnable() {
			public void run() {
				killAll(true);
			}
		});

	}
	
	public static void log(String x) { System.out.println(x);}
	
	public void setNumberOfThreads(int n) {
		log("setNumberOfThreads "+n);
		forcedNumberOfThreads = n>NMAXTHREADS ? NMAXTHREADS : n;
		updateNumOfThreads();
	}
	
	public void addOne() {
		log("addOne");
		final Worker job = factory.create(threads);
		job.cleanup.add( new Runnable() {
			public void run() {
				log("cleanup");
				if (threads.contains(job))
					threads.remove(job);
				updateNumOfThreads();
			}
		});
		threads.add(job);
		job.start();
	}

	
	public void deleteOne() {
		log("deleteOne");
		if (threads.size()==0)
			return;
		Worker selected = threads.get(0);
		int minprio = selected.priority;
		for(Worker t: threads) {
			if (t.priority<minprio) {
				minprio = t.priority;
				selected = t;
			}
		}
		threads.remove(selected);
		selected.exitRequest = true;
	}


	public void updateNumOfThreads() {
		try {
			int nt = forcedNumberOfThreads;
			if (nt<0)
				nt = ScheduleHelper.getNumOfThread();
	
			int cur = threads.size();
			if (cur<nt) {
				for(int i=cur; i<nt; i++) {
					log("nt="+nt+" c="+cur+ " add one");
					addOne();
				}
			}
			
			for(int i=nt; i<cur; i++) {
				log("nt="+nt+" c="+cur+ " delete one");
				deleteOne();
			}
			for(int i=0; i<threads.size(); i++)
				threads.get(i).position = i;

		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		

	}
	
	public void killAll(boolean andWait) {
		List<Worker> temp = new ArrayList<Worker>(threads);
		threads.clear();
		forcedNumberOfThreads = 0;
		
		for(Worker t: temp)
			t.exitRequest = true;
		if (andWait) {
			for(Worker t: temp) {
				try {
					t.join();
					log("join thread "+t.tag);
				} catch (Exception e) {
					log("ERROR on join thread "+t.tag);
					e.printStackTrace();
				}
			}
		}
	}

}


