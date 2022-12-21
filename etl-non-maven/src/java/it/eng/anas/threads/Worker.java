package it.eng.anas.threads;

import java.util.ArrayList;
import java.util.List;

import it.eng.anas.Log;

public abstract class Worker extends Thread {
	public String tag;
	public boolean exitRequest = false;
	public List<Runnable> cleanup = new ArrayList<Runnable>();
	public String workerStatus = "starting";
	public int index;
	
	public void log(String x) { 
		Log.log(tag+":"+x);
	}
	

	public Worker(String tag) {
		super(tag);
		this.tag = tag;
	}
	
	
	@Override
	public void run() {
		try {
			workerStatus = "started";
			execute();
		}
		catch(Exception e) {
			Log.log("ERRORE CHE FA USCIRE DAL WORKER");
			e.printStackTrace();
			workerStatus = e.getMessage();
		}

		workerStatus = "cleanup";
		for(Runnable c: cleanup) {
			try {
				if (c!=null)
					c.run();
			} 
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		workerStatus = "after cleanup, exit";
		
	}
	
	public abstract void execute() throws Exception;

}
