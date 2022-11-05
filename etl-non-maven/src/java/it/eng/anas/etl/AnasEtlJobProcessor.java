package it.eng.anas.etl;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.filenet.api.core.ContentTransfer;

import it.eng.anas.FileHelper;
import it.eng.anas.FilenetHelper;
import it.eng.anas.JSON;
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
//		if (job.operation.equals("getFolderMD"))
//			getFolderMDByPath(job);
		if (job.operation.equals("getFolderMD"))
			getFolderMD(job);

//		else if (job.operation.equals("getFolderDocs"))
//			getFolderDocs(job);

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
		Body body = caller.mapper.readValue(job.body, Body.class);
		if (body.folderId!=null)
			getFolderMDById(job);
		else
			getFolderMDByPath(job);
	}
	
	private void getFolderMDByPath(DBJob job) throws Exception {
		Body body = caller.mapper.readValue(job.body, Body.class);
		String os = body.os;
		String path = body.path;
		String outpath = folderMdPath(os, path);
		if (new File(outpath).exists()) {
			job.output ="già estratto";
			return;
		}
		FilenetHelper fnet = caller.getFilenetHelper();
		DbJobManager jobManager = caller.getJobManager();
		ObjectNode node = fnet.getFolderMetadataByPath(os, path, body.withdoc);
		fileHelper.saveJsonObject(outpath, node);
		
		
		if (body.maxrecursion>0) {
			
			List<String> children = fnet.getSubfoldersPaths(os, path);
			
			for(String child: children) {
				String childpath =  folderMdPath(os, child);
				if (new File(childpath).exists()) 
					continue;

				ObjectNode newbody = JSON.object(
					"os",           os,
					"path",         child,
					"maxrecursion", body.maxrecursion-1,
					"withdoc",      body.withdoc,
					"withcontent",  body.withcontent
				);

				
				jobManager.insertNew(
					job.queue, // queue
					job.priority-1, // priority
					"getFolderMD", // operation
					os, // key1
					child, // key2
					null, // key3
					job.id, //parentJob,
					JSON.string(newbody) // body
				);
			}
		}
		
		if (body.withdoc) {
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
				ObjectNode newbody = JSON.object(
					"os",          os,
					"path",        path,
					"docId",       docId,
					"withcontent", body.withcontent
				);

				jobManager.insertNew(
						job.queue, // queue
						job.priority-100, // priority
						"getDocMD", // operation
						os, // key1
						path, // key2
						docId, // key3
						job.id, //parentJob,
						JSON.string(newbody) // body
				);

			}
		}
	}
	
	
	private void getFolderMDById(DBJob job) throws Exception {
		Body body = caller.mapper.readValue(job.body, Body.class);
		String os = body.os;
		String id = body.folderId;
		String outpath = folderMdPath(os, id);
		if (new File(outpath).exists()) {
			job.output ="già estratto";
			return;
		}
		FilenetHelper fnet = caller.getFilenetHelper();
		DbJobManager jobManager = caller.getJobManager();
		ObjectNode node = fnet.getFolderMetadataById(os, id, body.withdoc);
		fileHelper.saveJsonObject(outpath, node);
		
		
		if (body.maxrecursion>0) {
			
			List<String> children = fnet.getSubfoldersId(os, id);
			
			for(String child: children) {
				String childpath =  folderMdPath(os, child);
				if (new File(childpath).exists()) 
					continue;

				ObjectNode newbody = JSON.object(
					"os",           os,
					"folderId",         child,
					"maxrecursion", body.maxrecursion-1,
					"withdoc",      body.withdoc,
					"withcontent",  body.withcontent
				);

				
				jobManager.insertNew(
					job.queue, // queue
					job.priority-1, // priority
					"getFolderMD", // operation
					os, // key1
					child, // key2
					null, // key3
					job.id, //parentJob,
					JSON.string(newbody) // body
				);
			}
		}
		
		if (body.withdoc) {
			ArrayNode arr = (ArrayNode) node.get("__documents__");
			if (arr.size()>0) {
				String docListPath = folderDocListPath(os, id);
				fileHelper.saveJsonObject(docListPath, arr);
			}
			for(int i=0; i<arr.size(); i++) {
				String docId = arr.get(i).asText();
				String docPath = docMdPath(os, docId);
				if (new File(docPath).exists()) {
					Log.etl.log(docId+" già presente");
					continue;
				}
				ObjectNode newbody = JSON.object(
					"os",          os,
					"path",        id,
					"docId",       docId,
					"withcontent", body.withcontent
				);

				jobManager.insertNew(
						job.queue, // queue
						job.priority-100, // priority
						"getDocMD", // operation
						os, // key1
						id, // key2
						docId, // key3
						job.id, //parentJob,
						JSON.string(newbody) // body
				);

			}
		}
	}

	private void getDocMD(DBJob job) throws Exception {
		Body body = caller.mapper.readValue(job.body, Body.class);
		String os = body.os;
		String docId = body.docId;

		String outpath = docMdPath(os, docId);
		if (new File(outpath).exists()) {
			job.output ="già estratto";
			return;
		}
		
		
		FilenetHelper fnet = caller.getFilenetHelper();
		DbJobManager jobManager = caller.getJobManager();
		ObjectNode node = fnet.getDocumentMetadata(os, docId);
		fileHelper.saveJsonObject(outpath, node);
		if (body.withcontent) {
			ObjectNode newbody = JSON.object(
				"os",    os,
				"path",  body.path,
				"docId", body.docId
			);

			jobManager.insertNew(
					job.queue, // queue
					job.priority-100, // priority
					"getContent", // operation
					os,         // key1
					body.path,  // key2
					docId,      // key3
					job.id,     //parentJob,
					JSON.string(newbody) // body
			);
			
		}
	}
	
	private void getContent(DBJob job) throws Exception {
		Body body = caller.mapper.readValue(job.body, Body.class);
		
		FilenetHelper fnet = caller.getFilenetHelper();
		List<ContentTransfer> contents = fnet.getContentTransfer(body.os, body.docId);
		for(ContentTransfer ct: contents) {
			String filename = ct.get_RetrievalName();
			InputStream s = ct.accessContentStream();
			String outpath = contentPath(body.os, body.docId, filename);
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
	
	public static class Body {
		public String os = null;
		public String folderId;
		public String docId;
		public String path = null;
		public Integer maxrecursion = null;
		public Boolean withdoc = null;
		public Boolean withcontent = null;
		
	}
	

}
