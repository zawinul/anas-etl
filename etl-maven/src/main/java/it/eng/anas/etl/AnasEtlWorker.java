package it.eng.anas.etl;

import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.sql.Connection;

import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.model.DBJob;

public class AnasEtlWorker extends DBConsumeWorker implements Cleanable {

	private DbJobManager dbmanager;
	private AnasEtlJobProcessor processor;
	
	public AnasEtlWorker(String tag, String queueName, int priority) {
		super(tag, queueName, priority);
		try {
			Connection con = DBConnectionFactory.defaultFactory.getConnection();
			dbmanager= new DbJobManager(con);
			processor = new AnasEtlJobProcessor(dbmanager);
			Cleaner cleaner = Cleaner.create();
			cleaner.register(this, new Runnable() {
				public void run() {
					clean();
				}
			});		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onJob(DBJob job) throws Exception {
		curJobDescription = job.id+": ["+job.operation+"]    R:"+job.nretry+" pars="+job.par1+","+job.par2+","+job.par3+" extra="+job.extra;
		log("AnasEtlJob onMessage "+tag+" "+processor);
		if (Math.random()<.05) // sometime fails
			throw new Exception("simulazione errore");
		
		processor.process(job);

		Utils.shortPause();
	}

	public void clean() {
		Log.db.log("cleaning AnasEtlWorker "+tag+" conn="+connection);
		try {
			if (connection!=null && !connection.isClosed())
				connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

}
