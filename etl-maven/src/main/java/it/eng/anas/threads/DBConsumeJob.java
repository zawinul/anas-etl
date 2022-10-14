package it.eng.anas.threads;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Log;
import it.eng.anas.QueueManager;
import it.eng.anas.Utils;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.model.DBJob;
import it.eng.anas.model.Esempio1;
import it.eng.anas.model.Model;

public class DBConsumeJob<T extends Model> extends Job {
	public String queueName;
	public ObjectMapper mapper;
	public DbJobManager manager;
	public boolean closed = false;
	public Class<? extends Model> tclass;
	public int waitOnBusy = 500;
	public Connection connection;
	
	
	public void log(String msg) {
		System.out.println(position+":"+tag+":"+msg);
	}

	public DBConsumeJob(String tag, String queueName, String type, int priority, Class<? extends Model> tclass) {

		super(tag, type, priority);
		
		this.queueName = queueName;
		this.tclass = tclass;
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

	public void onMessage(T obj) throws Exception {
		log("onMessage "+obj);
		int t = 500+(int)(Math.random()*5000);
		Utils.sleep(t);
	}

	public T beforeResend(T obj) {
		((Esempio1)obj).nretry++;
		return obj;
	}

	private boolean singleStep() throws Exception {
		DBJob job = manager.extract(queueName);
		if (job==null) {
			log("Coda vuota: "+queueName);
			return false;
		}

		log("onMessage|"+job.command);
		@SuppressWarnings("unchecked")
		T obj = (T) mapper.readValue(job.body, tclass);
		
		try {
			onMessage(obj);
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
