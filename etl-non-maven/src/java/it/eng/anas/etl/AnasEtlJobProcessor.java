package it.eng.anas.etl;

import java.util.HashMap;
import java.util.List;

import it.eng.anas.FileHelper;
import it.eng.anas.FilenetHelper;
import it.eng.anas.Utils;
import it.eng.anas.model.DBJob;

public class AnasEtlJobProcessor  {

	private AnasEtlWorker caller;
	private FileHelper fileHelper = new FileHelper();
	public AnasEtlJobProcessor(AnasEtlWorker caller) {

		this.caller = caller;
		dispatcher.put("startScanArchivi", job->new JobProcessorArchivi(caller).startScanArchivi(job));
		dispatcher.put("getArchiviFolder", job->new JobProcessorArchivi(caller).getArchiviFolder(job));
		dispatcher.put("getArchiviDoc", job->new JobProcessorArchivi(caller).getArchiviDoc(job));
		dispatcher.put("getArchiviContent", job->new JobProcessorArchivi(caller).getArchiviContent(job));

		dispatcher.put("startScanArchivi2",  job->new JobProcessorArchiviExt2(caller).startScanArchivi2(job));
		dispatcher.put("getArchiviNode",     job->new JobProcessorArchiviExt2(caller).getArchiviNode(job));
		dispatcher.put("getArchiviContent2", job->new JobProcessorArchiviExt2(caller).getArchiviContent2(job));
		
		dispatcher.put("startScanDBS", job->new JobProcessorDBS(caller).startScanDBS_DB(job));
		dispatcher.put("getDBSFolder", job->new JobProcessorDBS(caller).getDBSFolder_DB(job));
		dispatcher.put("getDBSDoc", job->new JobProcessorDBS(caller).getDBSDoc_DB(job));
		dispatcher.put("getDBSContent", job->new JobProcessorDBS(caller).getDBSContent(job));

	
	}
	
	
	public HashMap<String, JobProcessor> dispatcher = new HashMap<>();
	
	
	public void process(DBJob dbjob) throws Exception {
		AnasEtlJob job = (AnasEtlJob) dbjob;
		JobProcessor p = dispatcher.get(job.operation);
		if (p!=null)
			p.process(job);
		else
			throw new Exception("operation non riconosciuta: "+job.operation+", job="+Utils.getMapperOneLine().writeValueAsString(job));

	}

	
	@SuppressWarnings("unused")
	private void getListaDbs(AnasEtlJob job) throws Exception {
		FilenetHelper fnet = caller.getFilenetHelper();
		List<String[]> infos = fnet.getDbsInfo(job.os, job.folderId);
		String ret = "";
		for(String[] row: infos) {
			String r0 = row[0].replace("\n", " ").replace("|", " ");
			String r1 = row[1].replace("\n", " ").replace("|", " ");
			String r2 = row[2].replace("\n", " ").replace("|", " ");
			ret+=r0+"|"+r1+"|"+r2+"\n";
		}
		fileHelper.save("liste/"+job.key1+".txt", ret);
	}
	
		
	public interface JobProcessor {
		public abstract void process(AnasEtlJob job) throws Exception;
	}

}
