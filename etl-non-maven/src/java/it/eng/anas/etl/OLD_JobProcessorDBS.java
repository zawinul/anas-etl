package it.eng.anas.etl;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import it.eng.anas.FileHelper;
import it.eng.anas.FilenetHelper;
import it.eng.anas.Utils;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.db.FilenetDBHelper;

public class OLD_JobProcessorDBS {
	private AnasEtlWorker caller;
	private FileHelper fileHelper = new FileHelper();
	public OLD_JobProcessorDBS(AnasEtlWorker caller) {
		this.caller = caller;
	}
	
//	public void startScanDBS(AnasEtlJob job) throws Exception {
//		FilenetHelper fnet = caller.getFilenetHelper();
//		FilenetHelper.FolderInfo dbs;
//		String what = job.key1.toLowerCase();
//		String startId = (what.equals("progetti")) ? Utils.getConfig().idProgetti  : Utils.getConfig().idLavori;
//		dbs = fnet.traverseFolder("PDM", startId, true, false);
//		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
//
//		for(int i=0; i<dbs.children.size(); i++) {
//
//			FilenetHelper.SubFolderInfo sub = dbs.children.get(i);
//
//			if (job.buildDir)
//				fileHelper.getDir(sub.path);
//
//			int prio = 100000-i*1000;
//			String pathSegments[] = sub.path.split("/");
//			AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
//			subjob.operation = "getDBSFolder";
//			subjob.folderId = sub.id;
//			subjob.path = sub.path;
//			subjob.key1 = what;
//			subjob.key2 = pathSegments[pathSegments.length-1];
//			subjob.key3 = sub.path;
//			subjob.priority = prio;
//			subjob.os = "PDM";
//			jobManager.insertNew(subjob);
//		}
//	}

//	public void startScanDBS_DB(AnasEtlJob job) throws Exception {
//		FilenetDBHelper db = caller.getDB();
//		String what = job.key1.toUpperCase();
//		String startId = (what.equals("progetti")) ? Utils.getConfig().idProgetti  : Utils.getConfig().idLavori;
//		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
//
//		List<String[]> dbslist = db.getSubFolders("PDM", startId, what);
//		for(int i=0; i<dbslist.size(); i++) {
//			String dbs[] = dbslist.get(i);
//			String dbsId = dbs[0];
//			String path = dbs[1];
//			if (job.buildDir)
//				fileHelper.getDir(path);
//
//			if (!dbsId.startsWith("{"))
//				dbsId = FilenetDBHelper.dbid2guid(dbsId);
//			
//			int prio = 100000-i*1000;
//			String pathSegments[] = path.split("/");
//			AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
//			subjob.operation = "getDBSFolder";
//			subjob.folderId = dbsId;
//			subjob.path = path;
//			subjob.key1 = what;
//			subjob.key2 = pathSegments[pathSegments.length-1];
//			subjob.key3 = path;
//			subjob.priority = prio;
//			subjob.os = "PDM";
//			jobManager.insertNew(subjob);
//		}
//	}



	public void startScanDBS_DB(AnasEtlJob job) throws Exception {
		FilenetDBHelper db = caller.getDB();
		String what = job.key1.toLowerCase();
		String startId = (what.equals("progetti")) ? Utils.getConfig().idProgetti  : Utils.getConfig().idLavori;
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
		List<String> dbsIds = new ArrayList<>();
		db.getSubFolders("PDM", startId, what, new FilenetDBHelper.FolderListener() {
			
			@Override
			public void onFolder(String dbsId, String path) throws Exception {
//				if (job.buildDir)
//					fileHelper.getDir(path);

				if (!dbsId.startsWith("{"))
					dbsId = FilenetDBHelper.dbid2guid(dbsId);

				int i = dbsIds.size();
				int prio = 1000000-i*100;
				String pathSegments[] = path.split("/");
				AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
				subjob.operation = "getDBSFolder";
				subjob.folderId = dbsId;
				subjob.path = path;
				subjob.key1 = what;
				subjob.key2 = pathSegments[pathSegments.length-1];
				subjob.key3 = path;
				subjob.priority = prio+4;
				subjob.os = "PDM";
				jobManager.insertNew(subjob);
				dbsIds.add(dbsId);
				
			}
		});
	}
	
	public void getDBSDoc(AnasEtlJob job) throws Exception {
		String what = job.key1;
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

	
	public void getDBSDoc_DB(AnasEtlJob job) throws Exception {
		String what = job.key1;
		FilenetDBHelper db = caller.getDB();
		ObjectNode node = db.getDocProperties(job.os, job.docId);
		String outpath[] = docPath(what, job.docId, job.path);
		for(String p: outpath)	
			fileHelper.saveJsonObject(p, node);
		
		
		if (job.withcontent) {
			DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
			
			AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
			subjob.operation = "getDBSContent";
			subjob.priority = job.priority-10;

			jobManager.insertNew(subjob);
		}
	}

	public void getDBSContent(AnasEtlJob job) throws Exception {
		FilenetHelper fnet = caller.getFilenetHelper();
		String what = job.key1;
		List<FilenetHelper.ContentInfo> contents = fnet.getContentTransfer(job.os,  job.docId, true);
		for(int i=0;i<contents.size(); i++) {
			FilenetHelper.ContentInfo content = contents.get(i);
			String filename = i+"."+content.filename;
			String filepath = contentPath(what, job.docId, job.path, filename);
			fileHelper.save(filepath, content.stream);	
		}
	}
	
//	public void getDBSFolder(AnasEtlJob job) throws Exception {
//		FilenetHelper fnet = caller.getFilenetHelper();
//		FilenetHelper.FolderInfo node;
//		String what = job.key1.toLowerCase();
//		String dbs = job.key2;
//		node = fnet.traverseFolder("PDM", job.folderId, true, job.withdoc);
//		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
//		
//		for(int i=0; i<node.children.size(); i++) {
//			FilenetHelper.SubFolderInfo sub = node.children.get(i);
//			AnasEtlJob j = AnasEtlJob.createSubJob(job);
//			j.operation = "getDBSFolder";
//			j.folderId = sub.id;
//			j.path = sub.path;
//			j.key1 = what;
//			j.key2 = dbs;
//			j.key3 = sub.path;
//			j.priority = job.priority;
//			
//			jobManager.insertNew(j);
//
//			if (job.buildDir)
//				fileHelper.getDir(folderPath(what, sub.path));
//		}
//		if (job.withdoc) {
//			for(String docId: node.docs) {
//				AnasEtlJob j = AnasEtlJob.createSubJob(job);
//				j.operation = "getDBSDoc";
//				j.docId = docId;
//				j.path = job.path;
//				j.key1 = what;
//				j.key2 = dbs;
//				j.key3 = docId;
//				j.priority = job.priority-50;
//				jobManager.insertNew(j);
//			}
//		}
//	}


	public void getDBSFolder_DB(AnasEtlJob job) throws Exception {
		String what = job.key1.toLowerCase();
		String dbs = job.key2;
		FilenetDBHelper db = caller.getDB();
		String id = job.folderId;
		if (id.startsWith("{"))
			id = FilenetDBHelper.guid2dbid(id);

		//List<String[]> subfolders = db.getSubFolders("PDM", id, job.path);
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();

		db.getSubFolders("PDM", job.folderId, job.path, new FilenetDBHelper.FolderListener() {
			
			@Override
			public void onFolder(String subid, String subpath) throws Exception {
				AnasEtlJob j = AnasEtlJob.createSubJob(job);
				j.operation = "getDBSFolder";
				j.folderId = subid;
				j.path = subpath;
				j.key1 = what;
				j.key2 = dbs;
				j.key3 = subpath;
				j.priority = job.priority;
				
				jobManager.insertNew(j);

				if (job.buildDir)
					fileHelper.getDir(folderPath(what, subpath));

			}
		});

		if (job.withdoc) {
			db.getContainedDocumentsId("PDM", job.folderId, new FilenetDBHelper.DocIdListener() {
				
				@Override
				public void onDoc(String docId) throws Exception {
					AnasEtlJob j = AnasEtlJob.createSubJob(job);
					j.operation = "getDBSDoc";
					j.docId = docId;
					j.path = job.path;
					j.key1 = what;
					j.key2 = dbs;
					j.key3 = docId;
					j.priority = job.priority-1;
					jobManager.insertNew(j);
				}
			});
		}
	}

	
	public String folderPath(String what, String path) {
//		if (path.startsWith("/"))
//			path = path.substring(1);
//		if (path.startsWith("dbs/"))
//			path = path.substring(4);
//		return what+"/"+path;
		return path;
	}

	public String[] docPath(String what, String id, String path) {
		String fpath = folderPath(what, path);
		return new String[] {
			fpath+"/"+id+".json",
			what+"/_documents_/"+id.substring(34,37)+"/"+id+".json"
		};
	}

	public String contentPath(String what, String id, String path, String filename) {
		return what+"/_documents_/"+id.substring(34,37)+"/"+id+"."+filename;
	}


}
