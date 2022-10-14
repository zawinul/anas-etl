package it.eng.anas.threads;

public abstract class Job extends Thread {
	public Exception exitCause;
	public String tag;
	public String type;
	public int priority;
	public int position;
	public boolean exitRequest = false;
	public Runnable cleanup = null;
	
	public void log(String x) { 
		System.out.println(position+":"+tag+":"+x);
	}
	

	public Job(String tag, String type, int priority) {
		super();
		this.tag = tag;
		this.type = type;
		this.priority = priority;
	}
	
	public void onExit() {
		log("onExit");
	}
	
	
	
	@Override
	public void run() {
		try {
			execute();
		}
		catch(Exception e) {
			e.printStackTrace();
			exitCause = e;
		}

		try {
			onExit();
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			if (cleanup!=null)
				cleanup.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public abstract void execute() throws Exception;


}
