package it.eng.anas.etl;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Log;
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
		Log.main.log(position+":"+tag+":"+msg);
	}

	public DBConsumeWorker(String tag, String queueName, int priority) {

		super(tag, priority);
		this.queueName = queueName;
		
		jobManager = new DbJobManager();
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
			if (exitRequest)
				break;
			boolean d = singleStep();
			if (!d) {
				// probabilmente sono finiti i dati in coda
				// aspettiamo un po' prima di uscire e creare un altro thread
				Utils.longPause();
				break;
			}
		}
		log("end");
	}

	public void onJob(DBJob job) throws Exception {
		log("onJob "+job);
		Utils.shortPause();
	}

	private boolean singleStep()  {
		DBJob job = jobManager.extract(queueName);
		currentJob = job;
		if (job==null) {
			log("Coda vuota: "+queueName);
			return false;
		}

		log("onMessage|"+job.operation);
		
		try {
			onJob(job);
			log("ok");
			jobManager.ack(job);
			Utils.shortPause();
		} 
		catch (Exception e) {
			log("error on receive BL: "+e.getMessage());
			e.printStackTrace();
			jobManager.nack(job);
			Utils.shortPause();
		}
		return true;
		
	}

	public void close() {
		if (jobManager!=null)
			jobManager.close();
		jobManager = null;
	}
}
