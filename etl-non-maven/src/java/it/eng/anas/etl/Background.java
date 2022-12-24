package it.eng.anas.etl;

import java.sql.Connection;

import it.eng.anas.Event;
import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.SimpleDbOp;
import it.eng.anas.threads.ThreadManager;

public class Background {
	private static Background singleton;
	private Background() {}
	private boolean exiting = false;
	private Thread t;

	private void _start()  {
		Event.addListener("exit", new Runnable() {
			public void run() {
				exiting=true;
			}
		});

		t = new Thread() {
			public void run() {
				while(!exiting) {
					double t = (1+Math.random())*60000;
					Utils.sleep((int)t);
					loopStep();
				}
			}
		};
		t.start();
	}
	
	private void loopStep() {
		Utils.sleep(10000);
		
		Utils.refreshConfig();
		Connection con = null;
		try {
			con = DBConnectionFactory.defaultFactory.getConnection("background");
			updateAckNack(con);
		}catch(Exception e) {
			Log.log(e);
		}
		finally {
			DBConnectionFactory.close(con);
		}
		try {
			updateNWorkers();
		}catch(Exception e) {
			Log.log(e);
		}
	}

	private void updateAckNack(Connection connection) throws Exception {
		Log.log("# Update Ack/Nack");
		String fields = "jobid, priority, nretry, locktag, queue, operation, key1, key2, key3, creation, last_change, parent_job, duration, body, output";
		String sql = "insert into job_done ("+fields+") select "+fields+" from job where locktag='ack'";
		new SimpleDbOp(connection)
			.query(sql)
			.execute()
			.close()
			.throwOnError();
		
		sql = "insert into job_error ("+fields+") select "+fields+" from job where locktag='nack'";
		new SimpleDbOp(connection)
			.query(sql)
			.execute()
			.close()
			.throwOnError();
		
		sql = "delete from job where locktag=?";
		new SimpleDbOp(connection)
			.query(sql)
			.setString(1,  "ack")
			.execute()
			.close()
			.throwOnError();
	
		sql = "delete from job where locktag=?";
		new SimpleDbOp(connection)
			.query(sql)
			.setString(1, "nack")
			.execute()
			.close()
			.throwOnError();

	}
	
	private void updateNWorkers() throws Exception {
		Log.log("# Update nWorkers");
		ThreadManager.mainThreadManager.updateNumOfThreads();
	}
	
	public static void start()  {
		singleton = new Background();
		singleton._start();
	}
	
	public static void join()  {
		try {
			if (singleton!=null)
				singleton.t.join();
		} catch (InterruptedException e) {
			Log.log(e);
		}
	}
}
