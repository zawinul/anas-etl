package it.eng.anas.etl;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import it.eng.anas.FileHelper;
import it.eng.anas.FilenetHelper;
import it.eng.anas.Global;
import it.eng.anas.Utils;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.db.FilenetDBHelper;

public class JobProcessorArchivi {
	protected AnasEtlWorker caller;
	protected FileHelper fileHelper = new FileHelper();
	
	public JobProcessorArchivi(AnasEtlWorker caller) {
		this.caller = caller;
	}

	
	public void startScanArchivi(AnasEtlJob job) throws Exception {
		FilenetDBHelper db = caller.getDB();
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
		List<String> folders = new ArrayList<>();
		String startId = Utils.getConfig().idArchivi;
		db.getSubFolders("PDMASD", startId, "archivi", new FilenetDBHelper.FolderListener() {
			
			@Override
			public void onFolder(String folderId, String path) throws Exception {
	//			if (job.buildDir)
	//				fileHelper.getDir(path);
	
				if (!folderId.startsWith("{"))
					folderId = FilenetDBHelper.dbid2guid(folderId);
	
				int i = folders.size();
				int prio = 1000000-i*100;
				String pathSegments[] = path.split("/");
				AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
				subjob.operation = "getArchiviFolder";
				subjob.queue = "qdata";
				subjob.folderId = folderId;
				subjob.path = path;
				subjob.key1 = "archivi";
				subjob.key2 = pathSegments[pathSegments.length-1];
				subjob.key3 = path;
				subjob.priority = prio+4;
				subjob.os = "PDMASD";
				jobManager.insertNew(subjob);
				folders.add(folderId);	
			}
		});

	}
	
	public void getArchiviFolder(AnasEtlJob job) throws Exception {
		FilenetDBHelper db = caller.getDB();
		String id = job.folderId;
		if (id.startsWith("{"))
			id = FilenetDBHelper.guid2dbid(id);

		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();

		final IntContainer folderCount = new IntContainer();
		db.getSubFolders("PDMASD", job.folderId, job.path, new FilenetDBHelper.FolderListener() {
			@Override
			public void onFolder(String subid, String subpath) throws Exception {
				String pathComponents[] = subpath.split("/");
				if (pathComponents.length>=3 && Global.debug && folderCount.c>=3)
					return;
				String segment = pathComponents.length>=4 ? pathComponents[2]+"/"+pathComponents[3] : "-";
				AnasEtlJob j = AnasEtlJob.createSubJob(job);
				j.operation = "getArchiviFolder";
				j.queue = "qdata";
				j.folderId = subid;
				j.path = subpath;
				j.key1 = "archivi";
				j.key2 = segment;
				j.key3 = subpath;
				j.priority = job.priority;
				
				jobManager.insertNew(j);
				folderCount.c++;
				if (job.buildDir)
					fileHelper.getDir(folderPath(subpath));
			}
		});

		if (job.withdoc) {
			final IntContainer docCount = new IntContainer();
			db.getContainedDocumentsId("PDMASD", job.folderId, new FilenetDBHelper.DocIdListener() {
				
				@Override
				public void onDoc(String docId) throws Exception {
					if (Global.debug && docCount.c>5)
						return;
					else
						docCount.c++;
					if (!docId.startsWith("{"))
						docId = FilenetDBHelper.guid2dbid(docId);
					AnasEtlJob j = AnasEtlJob.createSubJob(job);
					String pathComponents[] = job.path.split("/");
					String segment = pathComponents.length>=4 ? pathComponents[2]+"/"+pathComponents[3] : "-";

					j.operation = "getArchiviDoc";
					j.queue = "qdata";
					j.docId = docId;
					j.path = job.path;
					j.key1 = "archivi";
					j.key2 = segment;
					j.key3 = docId;
					j.priority = job.priority-1;
					jobManager.insertNew(j);
				}
			});
		}
	}

	
	public void getArchiviDoc_API(AnasEtlJob job) throws Exception {
		FilenetHelper fnet = caller.getFilenetHelper();
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
		ObjectNode node = fnet.getDocumentMetadata(job.os, job.docId);
		
		String outpath[] = docPath(job.docId, job.path);
		for(String p: outpath)	
			fileHelper.saveJsonObject(p, node);
		
		if (job.withcontent) {
			AnasEtlJob newjob = AnasEtlJob.createSubJob(job);
			newjob.operation = "getArchiviContent";
			newjob.priority = job.priority-1;
			jobManager.insertNew(newjob);
		}
	}
	
	public void getArchiviDoc(AnasEtlJob job) throws Exception {
		FilenetDBHelper db = caller.getDB();
		ArrayNode node = db.getDocProperties(job.os, job.docId);
		String outpath[] = docPath(job.docId, job.path);
		for(String p: outpath)	
			if (node.size()>0)
				fileHelper.saveJsonObject(p, node.get(0));
		
		
		if (job.withcontent) {
			DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
			
			AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
			subjob.queue = "qcontent";
			subjob.operation = "getArchiviContent";
			subjob.priority = job.priority-1;

			jobManager.insertNew(subjob);
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
	
	
	
	public String folderPath(String path) {
		return path;
	}

	
	public String[] docPath(String id, String path) {
		return new String[] {
			path+"/"+id+".json"//,
			//"Archivi/_documents_/"+id.substring(34,37)+"/"+id+".json"
		};
	}

	public String contentPath(String id, String path, String filename) {
		//return "Archivi/_documents_/"+id.substring(34,37)+"/"+id+"."+filename;
		return path+"/"+id+"."+filename;

	}

	private static class IntContainer {
		public int c = 0;
	}


}
