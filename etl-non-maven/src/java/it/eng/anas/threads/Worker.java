package it.eng.anas.threads;

import java.util.ArrayList;
import java.util.List;

public abstract class Worker extends Thread {
	public String tag;
	public int priority;
	public int position;
	public boolean exitRequest = false;
	public List<Runnable> cleanup = new ArrayList<Runnable>();
	public String curJobDescription = "";

	public void log(String x) { 
		System.out.println(position+":"+tag+":"+x);
	}
	

	public Worker(String tag, int priority) {
		super();
		this.tag = tag;
		this.priority = priority;
	}
	
	
	@Override
	public void run() {
		try {
			execute();
		}
		catch(Exception e) {
			e.printStackTrace();
		}

		for(Runnable c: cleanup) {
			try {
				if (c!=null)
					c.run();
			} 
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public abstract void execute() throws Exception;

	public void close() {
		
	}
}
