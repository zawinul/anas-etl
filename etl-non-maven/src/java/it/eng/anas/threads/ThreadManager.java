package it.eng.anas.threads;

import it.eng.anas.Log;
import it.eng.anas.ScheduleHelper;

public class ThreadManager {
	public int NMAXTHREADS = 200;
	public Worker threads[] = new Worker[NMAXTHREADS];
	public WorkerFactory factory;
	public int forcedNumberOfThreads = 0;
	
	public static ThreadManager mainThreadManager;
	
	public ThreadManager(WorkerFactory factory) {
		this.factory = factory;
	}
	
	public static void log(String x) { 
		Log.etl.log(x);
	}
	
	public void setNumberOfThreads(int n) {
		log("setNumberOfThreads "+n);
		forcedNumberOfThreads = n>NMAXTHREADS ? NMAXTHREADS : n;
		updateNumOfThreads();
	}
	

	public Worker create(int i) {
		log("addOne");
		String tag = (""+(1000+i)).substring(1);
		final Worker wrk = factory.create("anas-etl", threads, "WRK-"+tag);
		wrk.cleanup.add( new Runnable() {
			public void run() {
				wrk.workerStatus = "cleanup";
				threads[i] = null;
				updateNumOfThreads();
			}
		});
		threads[i] = wrk;
		wrk.start();
		return wrk;
	}
	
	public void updateNumOfThreads() {
		try {
			int nt = forcedNumberOfThreads;
			if (nt<0)
				nt = ScheduleHelper.getNumOfThread();

			for(int i=0; i<nt;i++) {
				if (threads[i] == null) 
					threads[i] = create(i);
			}
			for(int i=nt; i<NMAXTHREADS;i++) {
				if (threads[i] != null) 
					threads[i].exitRequest = true;
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void killAll(boolean andWait) {
		Worker temp[] = new Worker[threads.length];
		for(var i=0; i<threads.length;i++) {
			temp[i] = threads[i];
			temp[i].exitRequest = true;
			threads[i] = null;
		}
		
		forcedNumberOfThreads = 0;
		
		if (andWait) {
			for(var i=0; i<temp.length;i++) {
				Worker t = temp[i];
				if (t!=null) {
					try {
						t.join();
						log("join thread "+t.tag);
					} catch (Exception e) {
						log("ERROR on join thread "+t.tag);
						//e.printStackTrace();
					}
					
				}
			}
		}
	}

}


