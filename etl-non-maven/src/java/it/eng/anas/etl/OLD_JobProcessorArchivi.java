package it.eng.anas.etl;

import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;

import it.eng.anas.FileHelper;
import it.eng.anas.FilenetHelper;
import it.eng.anas.Global;
import it.eng.anas.Utils;
import it.eng.anas.db.DbJobManager;

public class OLD_JobProcessorArchivi {
	private AnasEtlWorker caller;
	private FileHelper fileHelper = new FileHelper();
	public OLD_JobProcessorArchivi(AnasEtlWorker caller) {
		this.caller = caller;
	}

	public void startScanArchivi(AnasEtlJob job) throws Exception {
		FilenetHelper fnet = caller.getFilenetHelper();
		FilenetHelper.FolderInfo archivi;
		archivi = fnet.traverseFolder("PDMASD", Utils.getConfig().idArchivi, true, false);
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();

		for(int i=0; i<archivi.children.size(); i++) {

			FilenetHelper.SubFolderInfo sub = archivi.children.get(i);

			if (job.buildDir) 
				fileHelper.getDir(folderPath(sub.path));
			
			int prio = 100000-i*1000;
			String pathSegments[] = sub.path.split("/");
			AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
			subjob.operation = "getArchiviFolder";
			subjob.folderId = sub.id;
			subjob.path = sub.path;
			subjob.key1 = "archivi";
			subjob.key2 = pathSegments[pathSegments.length-1];
			subjob.key3 = "";
			subjob.priority = prio;
			subjob.os = "PDMASD";
			jobManager.insertNew(subjob);
		}
	}


	public void getArchiviDoc(AnasEtlJob job) throws Exception {
		FilenetHelper fnet = caller.getFilenetHelper();
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
		ObjectNode node = fnet.getDocumentMetadata(job.os, job.docId);
		
		String outpath[] = docPath(job.docId, job.path);
		for(String p: outpath)	
			fileHelper.saveJsonObject(p, node);
		
		if (job.withcontent) {
			AnasEtlJob newjob = AnasEtlJob.createSubJob(job);
			newjob.operation = "getArchiviContent";
			newjob.priority = job.priority-10;
			jobManager.insertNew(newjob);
		}
	}

	public void getArchiviContent(AnasEtlJob job) throws Exception {
		FilenetHelper fnet = caller.getFilenetHelper();
		
		List<FilenetHelper.ContentInfo> contents = fnet.getContentTransfer(job.os,  job.docId, true);
		for(int i=0;i<contents.size(); i++) {
			FilenetHelper.ContentInfo content = contents.get(i);
			String filename = i+"."+content.filename;
			String filepath = contentPath(job.docId, job.path, filename);
			fileHelper.save(filepath, content.stream);	
		}
	}
	
	public void getArchiviFolder(AnasEtlJob job) throws Exception {
		FilenetHelper fnet = caller.getFilenetHelper();
		FilenetHelper.FolderInfo node;
		node = fnet.traverseFolder("PDMASD", job.folderId, true, job.withdoc);
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
		
		for(int i=0; i<node.children.size(); i++) {
			int depth = job.path.split("/").length;
			if (Global.debug && depth>=3 && i>=3)
				break;
			FilenetHelper.SubFolderInfo sub = node.children.get(i);
			int prio = job.priority-1;
			AnasEtlJob j = AnasEtlJob.createSubJob(job);
			j.operation = "getArchiviFolder";
			j.folderId = sub.id;
			j.path = sub.path;
			j.key1 = "archivi";
			j.key2 = job.key2;
			j.key3 = sub.path;
			j.priority = prio;
			
			jobManager.insertNew(j);

			if (job.buildDir)
				fileHelper.getDir(sub.path);
		}
		if (job.withdoc) {
			for (int i=0;i<node.docs.size();i++) {
				if (Global.debug && (i>=2))
					break;
				String docId = node.docs.get(i);
				
				AnasEtlJob j = new AnasEtlJob();
				j.operation = "getArchiviDoc";
				j.docId = docId;
				j.path = job.path;
				j.key1 = "archivi";
				j.key2 = job.key2;
				j.key3 = docId;
				j.priority = job.priority-20;
				j.os = "PDMASD";
				j.withcontent = job.withcontent;
				j.parent_job = job.id;				
				jobManager.insertNew(j);
			}
		}
	}
	
	
	public String folderPath(String path) {
		return path;
	}

	
	public String[] docPath(String id, String path) {
		return new String[] {
			path+"/"+id+".json",
			"Archivi/_documents_/"+id.substring(34,37)+"/"+id+".json"
		};
	}

	public String contentPath(String id, String path, String filename) {
		return "Archivi/_documents_/"+id.substring(34,37)+"/"+id+"."+filename;
	}




}
