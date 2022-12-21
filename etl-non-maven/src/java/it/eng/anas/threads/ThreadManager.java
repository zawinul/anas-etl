package it.eng.anas.threads;

import it.eng.anas.Log;
import it.eng.anas.ScheduleHelper;
import it.eng.anas.etl.AnasEtlWorker;

public class ThreadManager {
	public int NMAXTHREADS = 200;
	public Worker threads[] = new Worker[NMAXTHREADS];
	public int forcedNumberOfThreads = 0;
	
	public static ThreadManager mainThreadManager;
	
	public ThreadManager() {
	}
	
	public static void log(String x) { 
		Log.log(x);
	}
	
	public void setNumberOfThreads(int n) {
		log("setNumberOfThreads "+n);
		forcedNumberOfThreads = n>NMAXTHREADS ? NMAXTHREADS : n;
		updateNumOfThreads();
	}
	

	public Worker create(int i) {
		log("addOne");
		String tag = (""+(1000+i)).substring(1);
		final Worker wrk = new AnasEtlWorker(tag);	
		wrk.index = i;
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
		forcedNumberOfThreads = 0;
		int n = 0;
		for(var i=0; i<threads.length;i++) {
			if (threads[i]!=null) {
				temp[n] = threads[i];
				temp[n].exitRequest = true;
				threads[i] = null;
				n++;
			}
		}
		
		
		if (andWait) {
			for(var i=0; i<n;i++) {
				try {
					log("join thread "+temp[i].tag);
					temp[i].join();
				} catch (Exception e) {
					log("ERROR on join thread "+temp[i].tag);
					//e.printStackTrace();
				}
			}
		}
	}

}


