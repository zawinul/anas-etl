package it.eng.anas.etl;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Utils;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.model.DBJob;
import it.eng.anas.threads.Worker;

public class DBConsumeWorker<T extends DBJob> extends Worker {
	public String queueName;
	public ObjectMapper mapper;
	public DbJobManager<T> jobManager;
	public boolean closed = false;
	public int waitOnBusy = 500;
	public T currentJob;
	private Class<T> tclass; 

	public void log(String msg) {
		if(currentJob!=null)
			super.log("job-"+currentJob.id+":"+msg);
	}

	public DBConsumeWorker(String tag, String queueName, int priority, Class<T> tclass) {

		super(tag, priority);
		this.tclass = tclass;
		this.queueName = queueName;
		
		jobManager = new DbJobManager<T>(tag, tclass);
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
				workerStatus = "exit request";
				break;
			}
			singleStep();
		}
		log("end");
	}

	public void onJob(T job) throws Exception {
		log("onJob "+job);
		workerStatus = "on job "+job.id+" "+job.operation;
		Utils.shortPause();
	}

	private void singleStep() throws Exception {
		T job = jobManager.extract(queueName);
		currentJob = job;
		if (job==null) {
			workerStatus = "coda vuota";
			Utils.shortPause();
			//log("Coda vuota: "+queueName);
			return;
		}

		log("onMessage "+job.operation);
		
		try {
			workerStatus = "on job "+job.id;
			onJob(job);
			log("ok");
			jobManager.ack(job, "ok");
			Utils.shortPause();
		} 
		catch (Exception e) {
			log("error on receive BL: "+e.getMessage());
			jobManager.nack(job, Utils.getStackTrace(e));
			workerStatus = e.getMessage();
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
