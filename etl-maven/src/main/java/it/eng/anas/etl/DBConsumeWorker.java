package it.eng.anas.etl;

import java.lang.ref.Cleaner;
import java.sql.Connection;
import java.sql.SQLException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.model.Config;
import it.eng.anas.model.DBJob;
import it.eng.anas.threads.Worker;

public class DBConsumeWorker extends Worker {
	public String queueName;
	public ObjectMapper mapper;
	public DbJobManager manager;
	public boolean closed = false;
	public int waitOnBusy = 500;
	public Connection connection;
	public DBJob currentJob;
	
	public void log(String msg) {
		Log.main.log(position+":"+tag+":"+msg);
	}

	public DBConsumeWorker(String tag, String queueName, int priority) {

		super(tag, priority);
		this.queueName = queueName;
		
		try {
			connection = DBConnectionFactory.defaultFactory.getConnection();
			manager = new DbJobManager(connection);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Cleaner cleaner = Cleaner.create();
		cleaner.register(this, new Runnable() {
			public void run() {
				clean();
			}
		});
		
		this.cleanup.add(new Runnable() {

			@Override
			public void run() {
				clean();				
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
				Config cfg = Utils.getConfig();
				Utils.longPause();
				break;
			}
		}
		close();
		log("end");
	}

	public void onJob(DBJob job) throws Exception {
		log("onJob "+job);
		int t = 500+(int)(Math.random()*5000);
		Utils.shortPause();
	}

	private boolean singleStep() throws Exception {
		Config cfg = Utils.getConfig();
		DBJob job = manager.extract(queueName);
		currentJob = job;
		if (job==null) {
			log("Coda vuota: "+queueName);
			return false;
		}

		log("onMessage|"+job.operation);
		
		try {
			onJob(job);
			log("ok");
			manager.ack(job);
			Utils.shortPause();
		} 
		catch (Exception e) {
			log("error on receive BL: "+e.getMessage());
			e.printStackTrace();
			manager.nack(job);
			Utils.shortPause();
		}
		return true;
		
	}

	public void close()  {
		if (!closed) {
			log("DBConsumeWorker close");
			try {
				if (connection!=null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			closed = true;
		}
	}	
	
	private void clean()  {
		log("DBConsumerWorker clean");
		try {
			if (connection!=null)
				if (!connection.isClosed()) 
					connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
