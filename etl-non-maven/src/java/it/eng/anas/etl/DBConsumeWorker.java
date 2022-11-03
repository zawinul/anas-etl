package it.eng.anas.etl;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Utils;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.model.DBJob;
import it.eng.anas.threads.Worker;

public class DBConsumeWorker extends Worker {
	public String queueName;
	public ObjectMapper mapper;
	public DbJobManager jobManager;
	public boolean closed = false;
	public int waitOnBusy = 500;
	public DBJob currentJob;
	
	public void log(String msg) {
		if(currentJob!=null)
			super.log("job-"+currentJob.id+":"+msg);
	}

	public DBConsumeWorker(String tag, String queueName, int priority) {

		super(tag, priority);
		this.queueName = queueName;
		
		jobManager = new DbJobManager(tag);
		final DBConsumeWorker t = this;
		cleanup.add(new Runnable() {
			public void run() {
				t.close();
			}
		});
	}
	
	public void execute() throws Exception {
		log("start");
		mapper = Utils.getMapper();
		while(true) {
			if (exitRequest) {
				status = "exit request";
				break;
			}
			singleStep();
		}
		log("end");
	}

	public void onJob(DBJob job) throws Exception {
		log("onJob "+job);
		status = "on job "+job.id+" "+job.operation;
		Utils.shortPause();
	}

	private void singleStep() throws Exception {
		DBJob job = jobManager.extract(queueName);
		currentJob = job;
		if (job==null) {
			status = "coda vuota";
			Utils.longPause();
			//log("Coda vuota: "+queueName);
			return;
		}

		log("onMessage "+job.operation);
		
		try {
			status = "on job "+job.id;
			onJob(job);
			log("ok");
			jobManager.ack(job, "ok");
			Utils.shortPause();
		} 
		catch (Exception e) {
			log("error on receive BL: "+e.getMessage());
			jobManager.nack(job, Utils.getStackTrace(e));
			status = e.getMessage();
			throw e;
		}
		finally {
			currentJob = null;
		}
	}

	public void close() {
		if (jobManager!=null)
			jobManager.close();
		jobManager = null;
	}
}
