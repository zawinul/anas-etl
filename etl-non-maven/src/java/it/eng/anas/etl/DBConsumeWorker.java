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
	
	public void log(String msg) {
		if(currentJob!=null)
			super.log("job-"+currentJob.id+":"+msg);
	}

	public DBConsumeWorker(String tag, String queueName, Class<T> tclass) {

		super(tag);
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
			Utils.shortPause();
		}
		log("end");
	}

	public void onJob(T job) throws Exception {
		log("onJob "+job);
		workerStatus = "on job "+job.id+" "+job.operation;
		Utils.shortPause();
	}

	private void singleStep() throws Exception {
		workerStatus = "getting job";
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
			workerStatus = "job "+job.id+" ack";
			jobManager.ack(job, "ok");
		} 
		catch (Exception e) {
			log("error on receive BL: "+e.getMessage());
			jobManager.nack(job, Utils.getStackTrace(e));
			workerStatus = "job "+job.id+" "+e.getMessage();
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
