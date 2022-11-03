package it.eng.anas.etl;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.filenet.api.core.ContentTransfer;

import it.eng.anas.FileHelper;
import it.eng.anas.FilenetHelper;
import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.model.DBJob;

public class AnasEtlJobProcessor  {

	private AnasEtlWorker caller;
	private FileHelper fileHelper = new FileHelper();
	public AnasEtlJobProcessor(AnasEtlWorker caller) {
		this.caller = caller;
	}
	
	public void process(DBJob job) throws Exception {
		if (job.operation.equals("getFolderMD"))
			getFolderMD(job);

		else if (job.operation.equals("getFolderDocs"))
			getFolderDocs(job);

		else if (job.operation.equals("getDocMD"))
			getDocMD(job);

		else if (job.operation.equals("getContent"))
			getContent(job);



//		else if (job.operation.equals("getClassStruct"))
//			getClassStruct(job);
		
		else {
			throw new Exception("operation non riconosciuta: "+job.operation+", job="+Utils.getMapperOneLine().writeValueAsString(job));
		}
		
	}
	
	private void getFolderMD(DBJob job) throws Exception {
		String os = job.par1;
		String path = job.par2;
		Extra extra = caller.mapper.readValue(job.extra, Extra.class);
		String outpath = folderMdPath(os, path);
		if (new File(outpath).exists()) {
			job.output ="già estratto";
			return;
		}
		FilenetHelper fnet = caller.getFilenetHelper();
		DbJobManager jobManager = caller.getJobManager();
		ObjectNode node = fnet.getFolderMetadata(os, path, extra.withdoc);
		fileHelper.saveJsonObject(outpath, node);
		
		
		if (extra.maxrecursion>0) {
			Extra newExtra = clone(extra);
			newExtra.maxrecursion--;
			
			List<String> children = fnet.getSubfolders(os, path);
			String jsonExtra = caller.mapper.writeValueAsString(newExtra);
			for(String child: children) {

				String childpath =  folderMdPath(os, child);
				if (new File(childpath).exists()) 
					continue;
				
				jobManager.insertNew(
					job.queue, // queue
					job.priority-1, // priority
					"getFolderMD", // operation
					os, // par1
					child, // par2
					null, // par3
					job.id, //parentJob,
					jsonExtra // extra
				);
			}
		}
		
		if (extra.withdoc) {
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
				Extra newExtra = new Extra();
				newExtra.path = path;
				newExtra.withcontent = extra.withcontent;
				String jextra = Utils.getMapperOneLine().writeValueAsString(newExtra);

				jobManager.insertNew(
						job.queue, // queue
						job.priority-100, // priority
						"getDocMD", // operation
						os, // par1
						docId, // par2
						null, // par3
						job.id, //parentJob,
						jextra // extra
				);

			}
//			Extra newExtra = clone(extra);
//			String jsonExtra = caller.mapper.writeValueAsString(newExtra);
//			String doclistpath =  folderDocListPath(os, path);
//			if (!(new File(doclistpath).exists())) { 
//
//				jobManager.insertNew(
//						job.queue, // queue
//						job.priority-100, // priority
//						"getFolderDocs", // operation
//						os, // par1
//						path, // par2
//						null, // par3
//						job.id, //parentJob,
//						jsonExtra // extra
//				);
//			}
		}
	}
	
	private void getFolderDocs(DBJob job) throws Exception {
		String os = job.par1;
		String path = job.par2;
		
		String outpath = folderDocListPath(os, path);
		if (new File(outpath).exists()) {
			job.output ="già estratto";
			return;
		}

		Extra extra = caller.mapper.readValue(job.extra, Extra.class);
		FilenetHelper fnet = caller.getFilenetHelper();
		DbJobManager jobManager = caller.getJobManager();
		Extra newExtra = new Extra();
		newExtra.path = path;
		newExtra.withcontent = extra.withcontent;
		String jextra = Utils.getMapperOneLine().writeValueAsString(newExtra);
		List<String> docIds = fnet.getDocumentsId(os, path);
		fileHelper.saveJsonObject(outpath, docIds);
		
		for(String docId: docIds) {
			String docPath = docMdPath(os, docId);
			if (new File(docPath).exists()) {
				Log.etl.log(docId+" già presente");
				continue;
			}
			jobManager.insertNew(
					job.queue, // queue
					job.priority-100, // priority
					"getDocMD", // operation
					os, // par1
					docId, // par2
					null, // par3
					job.id, //parentJob,
					jextra // extra
			);
		}
	}
	
	private void getDocMD(DBJob job) throws Exception {
		String os = job.par1;
		String docId = job.par2;

		String outpath = docMdPath(os, docId);
		if (new File(outpath).exists()) {
			job.output ="già estratto";
			return;
		}
		
		Extra extra = caller.mapper.readValue(job.extra, Extra.class);
		
		FilenetHelper fnet = caller.getFilenetHelper();
		DbJobManager jobManager = caller.getJobManager();
		ObjectNode node = fnet.getDocumentMetadata(os, docId);
		fileHelper.saveJsonObject(outpath, node);
		if (extra.withcontent) {
			Extra newExtra = new Extra();
			newExtra.path = extra.path;
			String jextra = Utils.getMapperOneLine().writeValueAsString(newExtra);
			jobManager.insertNew(
					job.queue, // queue
					job.priority-100, // priority
					"getContent", // operation
					os, // par1
					docId, // par2
					null, // par3
					job.id, //parentJob,
					jextra // extra
			);
			
		}
	}
	
	private void getContent(DBJob job) throws Exception {
		String os = job.par1;
		String docId = job.par2;
		
		@SuppressWarnings("unused")
		Extra extra = caller.mapper.readValue(job.extra, Extra.class);
		FilenetHelper fnet = caller.getFilenetHelper();
		List<ContentTransfer> contents = fnet.getContentTransfer(os, docId);
		for(ContentTransfer ct: contents) {
			String filename = ct.get_RetrievalName();
			InputStream s = ct.accessContentStream();
			String outpath = contentPath(os, docId, filename);
			fileHelper.save(outpath, s);
		}
		
	}
	
	private String folderMdPath(String os, String path) {
		return os+"/"+path+"/folder-metadata.json";
	}
	
	
	private String folderDocListPath(String os, String path) {
		return os+"/"+path+"/document-list.json";
	}

	private String docMdPath(String os, String docId) {
		return os+"/_documents/"+docId+".metadata.json";
	}

	private String contentPath(String os, String docId, String rname) {
		return os+"/_documents/"+docId+"."+rname;
	}
	
	public static class Extra {
		public String path = null;
		public Integer maxrecursion = null;
		public Boolean withdoc = null;
		public Boolean withcontent = null;
		
	}
	
	private static Extra clone(Extra x) {
		Extra r = Utils.getMapper().convertValue(x, Extra.class);
		return r;
	}

}
