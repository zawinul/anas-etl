package it.eng.anas.etl;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.FilenetHelper;
import it.eng.anas.Utils;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.db.FilenetDBHelper;

public class AnasEtlWorker extends DBConsumeWorker<AnasEtlJob>  {

	private AnasEtlJobProcessor processor;
	private FilenetHelper filenet = null;
	private FilenetDBHelper filenetdb = null;
	public ObjectMapper mapper = Utils.getMapper();
	
	public AnasEtlWorker(String tag) {
		super(tag, AnasEtlJob.class);
		processor = new AnasEtlJobProcessor(this);
	}
	
	public FilenetDBHelper getDB() throws Exception {
		if (filenetdb==null) {
			filenetdb = new FilenetDBHelper(tag);
		}
		return filenetdb;
	}

	public FilenetHelper getFilenetHelper() throws Exception {
		if (filenet==null) {
			filenet = new FilenetHelper();
			filenet.initFilenetAuthentication();
		}
		return filenet;
	}

	
	public DbJobManager<AnasEtlJob> getJobManager() throws Exception {
		return jobManager;
	}

	@Override
	public void onJob(AnasEtlJob job) throws Exception {
		log("AnasEtlJob onMessage "+job.id);
		if (Utils.getConfig().simulazioneErrori)
			if (Math.random()<.01) // fallisce l'1% delle operazioni
				throw new Exception("simulazione errore");
		
		processor.process(job);

		Utils.shortPause();
	}

}
