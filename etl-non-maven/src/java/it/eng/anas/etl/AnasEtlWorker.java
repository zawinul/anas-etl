package it.eng.anas.etl;

import it.eng.anas.FilenetHelper;
import it.eng.anas.Utils;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.model.DBJob;

public class AnasEtlWorker extends DBConsumeWorker  {

	private AnasEtlJobProcessor processor;
	private FilenetHelper filenet = null;
	
	public AnasEtlWorker(String tag, String queueName, int priority) {
		super(tag, queueName, priority);
		processor = new AnasEtlJobProcessor(this);
	}
	
	public FilenetHelper getFilenetHelper() throws Exception {
		if (filenet==null) {
			filenet = new FilenetHelper();
			filenet.initFilenetAuthentication();
		}
		return filenet;
	}

	
	public DbJobManager getJobManager() throws Exception {
		return jobManager;
	}

	@Override
	public void onJob(DBJob job) throws Exception {
		curJobDescription = job.id+": ["+job.operation+"]    R:"+job.nretry+" pars="+job.par1+","+job.par2+","+job.par3+" extra="+job.extra;
		log("AnasEtlJob onMessage "+tag+" "+processor);
		if (Utils.getConfig().simulazioneErrori)
			if (Math.random()<.05) // sometime fails
				throw new Exception("simulazione errore");
		
		processor.process(job);

		Utils.shortPause();
	}

}
