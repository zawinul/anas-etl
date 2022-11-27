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

//		if (job.operation.equals("getArchiviFolder"))
//			new JobProcessorArchivi(caller).getArchiviFolder(job);
//		else  if (job.operation.equals("startScanArchivi"))
//			new JobProcessorArchivi(caller).startScanArchivi(job);
//		else if (job.operation.equals("getArchiviDoc"))
//			new JobProcessorArchivi(caller).getArchiviDoc(job);
//		
//		else if (job.operation.equals("getFolderMD"))
//			getFolderMD(job);
//		else if (job.operation.equals("getDocMD"))
//			getDocMD(job);
//		else if (job.operation.equals("getContent"))
//			getContent(job);
//
//		
//		else if (job.operation.equals("getClassStruct"))
//			getClassStruct(job);
//		
//		else if (job.operation.equals("getListaDbs"))
//			getListaDbs(job);
//		else {
//		}
		
	}

//	private void getFolderMD(AnasEtlJob job) throws Exception {
//		if (job.folderId!=null)
//			getFolderMDById(job);
////		else
////			getFolderMDByPath(job);
//	}
	
	private void getClassStruct(AnasEtlJob job) throws Exception {
		
	}
	
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
	
	
	
	

	/*
	private void getFolderMDByPath(AnasEtlJob job) throws Exception {
		Config config = Utils.getConfig();
		String os = job.os;
		String path = job.path;

		FilenetHelper fnet = caller.getFilenetHelper();
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
		ObjectNode node = fnet.getFolderMetadataByPath(os, path, job.withdoc);
		fileHelper.saveJsonObject(outpath, node);
		int level = 0;
		try {
			level = Integer.parseInt(job.key3);
		}catch(Exception e) {}
		
		if (job.maxrecursion>0) {
			
			List<String> children = fnet.getSubfoldersPaths(os, path);
			
			for(String child: children) {
				String childpath =  folderMdPath(os, child);
				if (new File(childpath).exists()) 
					continue;

				AnasEtlJob newjob = AnasEtlJob.clone(job)
				
					.withPriority(job.priority+config.priority.folderDelta)
					.withOperation("getFolderMD")
					.withKeys(os,  child, (level+1)+"")
					.withParentJob(job.id)
					
					.withPath(child)
					.withMaxrecursion(job.maxrecursion-1)
				;
				
				jobManager.insertNew(newjob);
			}
		}
		
		if (job.withdoc) {
			ArrayNode arr = (ArrayNode) node.get("__documents__");
			if (arr.size()>0) {
				String docListPath = folderDocListPath(os, path);
				fileHelper.saveJsonObject(docListPath, arr);
			}
			for(int i=0; i<arr.size(); i++) {
				String docId = arr.get(i).asText();
				String docPath = docMdPath(os, docId);
				if (new File(docPath).exists()) {
					Log.etl.log(docId+" già presente");
					continue;
				}
//				ObjectNode newbody = JSON.object(
//					"os",          os,
//					"path",        path,
//					"docId",       docId,
//					"withcontent", job.withcontent
//				);
//
//				jobManager.insertNew(
//						job.queue, // queue
//						config.priority.getDocMD, // priority
//						"getDocMD", // operation
//						os, // key1
//						path, // key2
//						docId, // key3
//						job.id, //parentJob,
//						JSON.string(newbody) // body
//				);
				
				AnasEtlJob newjob = AnasEtlJob.clone(job)
					.withOperation("getDocMD")
					.withPath(path)
					.withDocId(docId)
					.withKeys(os, path, docId)
					.withPriority(config.priority.getDocMD)
					.withParentJob(job.id);
				jobManager.insertNew(newjob);


			}
		}
	}
	*/
	
//	private void getFolderMDById(AnasEtlJob job) throws Exception {
//		Config config = Utils.getConfig();
//		String os = job.os;
//		String id = job.folderId;
//
//		FilenetHelper fnet = caller.getFilenetHelper();
//		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
//		ObjectNode node = fnet.getFolderMetadataById(os, id, job.withdoc);
//		String path = node.get("PathName").asText();
//		//String outpath = folderMdPath(os, id);
//		String dirPath = os+"/"+path;
//
//		boolean isDbsParent = path.split("/").length == 2;
//		boolean isDbs = path.split("/").length == 3;
//		if (isDbs)
//			fileHelper.saveJsonObject(dirPath+"/folder-metadata.json", node);
//		else if (job.buildDir)
//			fileHelper.getDir(dirPath);
//
//		int level = 0;
//		try {
//			level = Integer.parseInt(job.key3);
//		}catch(Exception e) {}
//
//		
//		if (job.maxrecursion>0) {
//			
//			List<String[]> children = fnet.getSubfoldersIdAndPath(os, id);
//			if (path.equals("/dbs/lavori") || path.equals("/dbs/progetti")) {
//				// ne seleziono n a caso
//				List<String[]> saved = children;
//				children = new ArrayList<String[]>();
//				for(int i=0; i<10; i++) {
//					int sel = (int) (Math.floor(Math.random()*saved.size()));
//					children.add(saved.get(sel));
//					saved.remove(sel);
//				}
//			}
//			
//			for(int i=0; i<children.size(); i++) {
//				String child[] = children.get(i);
//				String cId = child[0];
//				String cPath = child[1];
//				int nextPriority = 0;
//				if (isDbsParent)
//					nextPriority = 1000000-i*1000;
//				else
//					nextPriority = job.priority-100;
//				
//				AnasEtlJob newjob = AnasEtlJob.createSubJob(job)
//					.withOperation("getFolderMD")
//					.withFolderId(cId)
//					.withMaxrecursion(job.maxrecursion-1)
//					.withPriority(nextPriority)
//					.withKeys(os,  cPath,  ""+(level+1));
//
//				jobManager.insertNew(newjob);
//			}
//		}
//		
//		if (job.withdoc) {
//			ArrayNode arr = (ArrayNode) node.get("__documents__");
////			if (arr.size()>0) {
////				String docListPath = folderDocListPath(os, path);
////				fileHelper.saveJsonObject(docListPath, arr);
////			}
//			for(int i=0; i<arr.size(); i++) {
//				String docId = arr.get(i).asText();
////				String docPath = docMdPath(os, docId);
////				if (new File(docPath).exists()) {
////					Log.etl.log(docId+" già presente");
////					continue;
////				}
//				int nextPriority = job.priority-5;
//
//				AnasEtlJob newjob = AnasEtlJob.createSubJob(job)
//					.withOperation("getDocMD")
//					.withPriority(nextPriority)
//					.withPath(path)
//					.withDocId(docId)
//					.withKeys(os,  path,  docId);
//	
//				jobManager.insertNew(newjob);
//			}
//		}
//	}

//	private void getDocMD(AnasEtlJob job) throws Exception {
//		Config config = Utils.getConfig();
//		String os = job.os;
//		String docId = job.docId;
//
////		String outpath = docMdPath(os, docId);
////		if (new File(outpath).exists()) {
////			job.output ="già estratto";
////			return;
////		}
//		
//		
//		FilenetHelper fnet = caller.getFilenetHelper();
//		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
//		ObjectNode node = fnet.getDocumentMetadata(os, docId);
//		
//		String outpath[] = docMdPath(os, docId, job.path);
//		for(String p: outpath)	
//			fileHelper.saveJsonObject(p, node);
//		
//		if (job.withcontent) {
//			int nextPriority = job.priority-5;
//
//			AnasEtlJob newjob = AnasEtlJob.createSubJob(job)
//				.withOperation("getContent")
//				.withPriority(nextPriority)
//				.withPath(job.path)
//				.withDocId(job.docId)
//				.withKeys(os, job.path, docId)
//
//			jobManager.insertNew(newjob);
//		}
//	}
//	
//	private void getContent(AnasEtlJob job) throws Exception {
//		
//		FilenetHelper fnet = caller.getFilenetHelper();
//		List<ContentTransfer> contents = fnet.getContentTransfer(job.os, job.docId);
//		for(ContentTransfer ct: contents) {
//			String filename = ct.get_RetrievalName();
//			InputStream s = ct.accessContentStream();
//			String outpath[] = contentPath(job.os, job.docId, filename);
//			for(String p: outpath)
//				fileHelper.save(p, s);
//		}
//		
//	}
	
//	private String folderMdPath(String os, String path) {
//		return os+"/"+path+"/folder-metadata.json";
//	}
	
	
//	private String folderDocListPath(String os, String path) {
//		return os+"/"+path+"/document-list.json";
//	}

//	private String[] docMdPath(String os, String docId, String path) {
//		return new String[] {
//			os+"/"+path+"/"+docId+".json",
//			os+"/_documents/"+docId.substring(1,4)+"/"+docId+".metadata.json"
//		};
//	}
//
//	private String[] contentPath(String os, String docId, String rname) {
//		return new String[] {
//			os+"/_documents/"+docId.substring(1,4)+"/"+docId+"."+rname
//		};
//	}
	
	public interface JobProcessor {
		public abstract void process(AnasEtlJob job) throws Exception;
	}

}
