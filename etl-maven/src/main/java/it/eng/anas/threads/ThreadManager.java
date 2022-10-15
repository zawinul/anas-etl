package it.eng.anas.threads;

import java.util.ArrayList;
import java.util.List;

import it.eng.anas.ScheduleHelper;

public class ThreadManager {
	public int NMAXTHREADS = 200;
	public List<Job> threads = new ArrayList<Job>();
	public JobFactory factory;
	public int forcedNumberOfThreads = 0;
	public ThreadManager(JobFactory factory) {
		this.factory = factory;
	}
	
	public static void log(String x) { System.out.println(x);}
	
	public void setNumberOfThreads(int n) {
		log("setNumberOfThreads "+n);
		forcedNumberOfThreads = n>NMAXTHREADS ? NMAXTHREADS : n;
		updateNumOfThreads();
	}
	
	public void addOne() {
		log("addOne");
		final Job job = factory.create(threads);
		job.cleanup = new Runnable() {
			@Override
			public void run() {
				log("cleanup");
				if (threads.contains(job))
					threads.remove(job);
				updateNumOfThreads();
			}
		};
		threads.add(job);
		job.start();
	}

	
	public void deleteOne() {
		log("deleteOne");
		if (threads.size()==0)
			return;
		Job selected = threads.get(0);
		int minprio = selected.priority;
		for(Job t: threads) {
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
		List<Job> temp = new ArrayList<Job>(threads);
		threads.clear();
		forcedNumberOfThreads = 0;
		
		for(Job t: temp)
			t.exitRequest = true;
		if (andWait) {
			for(Job t: temp) {
				try {
					t.join();
					log("join thread "+t.tag);
				} catch (InterruptedException e) {
					log("ERROR on join thread "+t.tag);
					e.printStackTrace();
				}
			}
		}
	}

//	public  class JobThread extends Thread {
//		public Exception exitCause;
//		public String tag;
//		public String queue;
//		public String type;
//		int priority;
//		int position;
//		public boolean exitRequest = false;
//		
//		public JobThread(String tag, String queue, String type, int priority, Integer maxrun) {
//			super();
//			this.tag = tag;
//			this.queue = queue;
//			this.type = type;
//			this.priority = priority;
//		}
//
//		public void log(String x) { System.out.println(tag+": "+x); }
//		
//		public void onExit() {
//			log("onExit");
//			if (threads.contains(this))
//				threads.remove(this);
//			updateNumOfThreads();
//		}
//		
//		
//		
//		@Override
//		public void run() {
//			try {
//				run2();
//			}
//			catch(Exception e) {
//				e.printStackTrace();
//				exitCause = e;
//			}
//			finally {
//				onExit();
//			}
//		}
//		
//		public void run2() {
//			log("start");
//			while(true) {
//				if (exitRequest)
//					break;
//				log("in loop");
//				
//				Utils.sleep(3000);
//			}
//			log("end");
//		}
//	}
}


