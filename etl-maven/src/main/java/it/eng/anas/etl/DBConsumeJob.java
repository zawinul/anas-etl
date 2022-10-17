package it.eng.anas.etl;

import java.sql.Connection;
import java.sql.SQLException;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.FileHelper;
import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.model.DBJob;
import it.eng.anas.threads.Job;

public class DBConsumeJob extends Job {
	public String queueName;
	public ObjectMapper mapper;
	public DbJobManager manager;
	public boolean closed = false;
	public int waitOnBusy = 500;
	public Connection connection;
	
	public void log(String msg) {
		Log.main.log(position+":"+tag+":"+msg);
	}

	public DBConsumeJob(String tag, String queueName, int priority) {

		super(tag, queueName, priority);
		
		this.queueName = queueName;
		try {
			connection = DBConnectionFactory.defaultFactory.getConnection();
			manager = new DbJobManager(connection);
		} catch (Exception e) {
			e.printStackTrace();
		}
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
				Utils.randomSleep(15000, 60000);
				break;
			}
		}
		close();
		log("end");
	}

	public void onMessage(DBJob job) throws Exception {
		log("onMessage "+job);
		int t = 500+(int)(Math.random()*5000);
		Utils.sleep(t);
	}

	private boolean singleStep() throws Exception {
		DBJob job = manager.extract(queueName);
		if (job==null) {
			log("Coda vuota: "+queueName);
			return false;
		}

		log("onMessage|"+job.operation);
		
		try {
			onMessage(job);
			log("ok");
			manager.ack(job);
		} catch (Exception e) {
			log("error on receive BL: "+e.getMessage());
			manager.nack(job);
			e.printStackTrace();
		}
		Utils.sleep(waitOnBusy);
		return true;
		
	}

	public void close()  {
		if (!closed) {
			log("DBJob close");
			try {
				if (connection!=null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			closed = true;
		}
	}	

}
