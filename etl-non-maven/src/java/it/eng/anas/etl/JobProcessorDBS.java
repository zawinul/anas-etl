package it.eng.anas.etl;

import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;

import it.eng.anas.FileHelper;
import it.eng.anas.FilenetHelper;
import it.eng.anas.Utils;
import it.eng.anas.db.DbJobManager;

public class JobProcessorDBS {
	private AnasEtlWorker caller;
	private FileHelper fileHelper = new FileHelper();
	public JobProcessorDBS(AnasEtlWorker caller) {
		this.caller = caller;
	}

	public void startScanDBS(AnasEtlJob job) throws Exception {
		FilenetHelper fnet = caller.getFilenetHelper();
		FilenetHelper.FolderInfo dbs;
		String what = job.key1.toLowerCase();
		String startId = (what.equals("progetti")) ? Utils.getConfig().idProgetti  : Utils.getConfig().idLavori;
		dbs = fnet.traverseFolder("PDM", startId, true, false);
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();

		for(int i=0; i<dbs.children.size(); i++) {

			FilenetHelper.SubFolderInfo sub = dbs.children.get(i);

			if (job.buildDir)
				fileHelper.getDir(sub.path);

			int prio = 100000-i*1000;
			String pathSegments[] = sub.path.split("/");
			AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
			subjob.operation = "getDBSFolder";
			subjob.folderId = sub.id;
			subjob.path = sub.path;
			subjob.key1 = what;
			subjob.key2 = pathSegments[pathSegments.length-1];
			subjob.key3 = sub.path;
			subjob.priority = prio;

			jobManager.insertNew(subjob);
		}
	}

	
	
	public void getDBSDoc(AnasEtlJob job) throws Exception {
		String what = job.key1.toLowerCase();
		FilenetHelper fnet = caller.getFilenetHelper();
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
		ObjectNode node = fnet.getDocumentMetadata(job.os, job.docId);
		
		String outpath[] = docPath(what, job.docId, job.path);
		for(String p: outpath)	
			fileHelper.saveJsonObject(p, node);
		
		if (job.withcontent) {

			AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
			subjob.operation = "getDBSContent";
			subjob.priority = job.priority-10;

			jobManager.insertNew(subjob);
		}
	}

	public void getDBSContent(AnasEtlJob job) throws Exception {
		FilenetHelper fnet = caller.getFilenetHelper();
		String what = job.key1.toLowerCase();
		List<FilenetHelper.ContentInfo> contents = fnet.getContentTransfer(job.os,  job.docId, true);
		for(int i=0;i<contents.size(); i++) {
			FilenetHelper.ContentInfo content = contents.get(i);
			String filename = i+"."+content.filename;
			String filepath = contentPath(what, job.docId, job.path, filename);
			fileHelper.save(filepath, content.stream);	
		}
	}
	
	public void getDBSFolder(AnasEtlJob job) throws Exception {
		FilenetHelper fnet = caller.getFilenetHelper();
		FilenetHelper.FolderInfo node;
		String what = job.key1.toLowerCase();
		String dbs = job.key2;
		node = fnet.traverseFolder("PDM", job.folderId, true, job.withdoc);
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
		
		for(int i=0; i<node.children.size(); i++) {
			FilenetHelper.SubFolderInfo sub = node.children.get(i);
			AnasEtlJob j = AnasEtlJob.createSubJob(job);
			j.operation = "getDBSFolder";
			j.folderId = sub.id;
			j.path = sub.path;
			j.key1 = what;
			j.key2 = dbs;
			j.key3 = sub.path;
			j.priority = job.priority;
			
			jobManager.insertNew(j);

			if (job.buildDir)
				fileHelper.getDir(folderPath(what, sub.path));
		}
		if (job.withdoc) {
			for(String docId: node.docs) {
				AnasEtlJob j = AnasEtlJob.createSubJob(job);
				j.operation = "getDBSDoc";
				j.docId = docId;
				j.path = job.path;
				j.key1 = what;
				j.key2 = dbs;
				j.key3 = docId;
				j.priority = job.priority-20;
				jobManager.insertNew(j);
			}
		}
	}

	
	public String folderPath(String what, String path) {
		return what+"/"+path;
	}

	public String[] docPath(String what, String id, String path) {
		if (path.startsWith("dbs/"))
			path = path.substring(4);
		return new String[] {
			what+"/"+path+"/"+id+".json",
			what+"/_documents_/"+id.substring(34,37)+"/"+id+".json"
		};
	}

	public String contentPath(String what, String id, String path, String filename) {
		return what+"/_documents_/"+id.substring(34,37)+"/"+id+"."+filename;
	}


}
