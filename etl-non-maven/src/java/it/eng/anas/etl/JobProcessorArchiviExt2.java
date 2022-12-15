package it.eng.anas.etl;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;

import it.eng.anas.FileHelper;
import it.eng.anas.FilenetHelper;
import it.eng.anas.Global;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.db.FilenetDBHelper;
import it.eng.anas.db.ResultSetToJson;
import it.eng.anas.db.SimpleDbOp;

public class JobProcessorArchiviExt2 extends JobProcessorArchivi {
	public Connection fnconn;
	public FilenetDBHelper db;
	public FileHelper fileh = new FileHelper();
	
	// d>0 => esplora in profondità 
	// d<0 => esplora in ampiezza
	public static int CHILD_DELTA = 1; 

	public static int CONTENT_DELTA = 0;
	
	private static HashMap<String, String> _docfieldmap = null;
	public  static HashMap<String, String> getDocFieldMap() throws Exception {
		if (_docfieldmap==null) {
			FilenetDBHelper fdb = new FilenetDBHelper("temp");
			fdb.setOs("pdmasd");
			_docfieldmap = fdb.getDocFieldMap();
			new FileHelper().saveJsonObject("archivi2/field-map.json", _docfieldmap);
		}
		return _docfieldmap;
	}
 	
	public JobProcessorArchiviExt2(AnasEtlWorker caller) {
		super(caller);
		try {
			db = caller.getDB();
			db.setOs("pdmasd");
			fnconn = db.getConnection();
			getDocFieldMap();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public void startScanArchivi2(AnasEtlJob job) throws Exception {
		
		String query="select object_id, u74_numero, U62_CODICEIDENTIFICATIVOARCHIV from docversion "
				+ "where U75_ELEMENTOARCHIVISTICOPARENT is null "
				+ "and U62_CODICEIDENTIFICATIVOARCHIV is not null "
				+ "and u76_titolo is not null";

		SimpleDbOp op1 = new SimpleDbOp(fnconn)
			.query(query)
			.executeQuery()
			.throwOnError();
		
		
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
		while(op1.next()) {
			String id = op1.getString("object_id");
			String archivio = op1.getString("U62_CODICEIDENTIFICATIVOARCHIV");

			AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
			subjob.operation = "getArchiviNode";
			subjob.queue = "qdata";
			subjob.path = archivio;
			subjob.key1 = "archivi2";
			subjob.key2 = archivio;
			subjob.key3 = id;
			subjob.priority = 0;
			subjob.os = "PDMASD";
			subjob.docId = id;
			subjob.dir = null;
			jobManager.insertNew(subjob);
		}
		op1.close().throwOnError();
	}
	
	public void getArchiviNode(AnasEtlJob job) throws Exception {
		
		ResultSetToJson j = new ResultSetToJson();
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();

		int level = job.path.split("\\.").length;

		String query="select * from docversion where object_id=?";
		SimpleDbOp op1 = new SimpleDbOp(fnconn)
			.query(query)
			.setString(1, job.docId)
			.executeQuery()
			.throwOnError();

		
		if (!op1.next())
			throw new Exception("doc not found "+job.docId);

		String nextdir=null;
		if (level==1) 
			nextdir=job.path;
		else if (level==2)
			nextdir = job.dir+"/"+op1.getInt("u74_numero")+" "+op1.getString("u76_titolo");
		else
			nextdir = job.dir+"/"+op1.getInt("u74_numero");
		
		boolean hasContent = op1.getLong("CONTENT_SIZE")>0;
		ObjectNode node = j.extractSingleRowWithTypes(op1.getResultSet(), getDocFieldMap());
		op1.close().throwOnError();
		
		String childQuery="select object_id, u74_numero,u76_titolo from docversion where U75_ELEMENTOARCHIVISTICOPARENT=?";
		SimpleDbOp childOp = new SimpleDbOp(fnconn)
			.query(childQuery)
			.setString(1, job.docId)
			.executeQuery()
			.throwOnError();
		
		int nchild=0;
		while(childOp.next()) {
			nchild++;
			String childId  = childOp.getString("object_id");
			int numero = childOp.getInt("u74_numero");
			String titolo = childOp.getString("u76_titolo");
			int subprio = job.priority+CHILD_DELTA;
			
			if (titolo.trim().toLowerCase().equals("nuove costruzioni"))
				subprio+=100;
			if (titolo.trim().toLowerCase().equals("ancona"))
				subprio+=200;
			if (titolo.trim().toLowerCase().equals("perugia"))
				subprio-=200;
			
			String nextpath = job.path+"."+numero;
			AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
			subjob.operation = "getArchiviNode";
			subjob.queue = "qdata";
			subjob.path = nextpath;
			subjob.key1 = "archivi2";
			subjob.key2 = nextpath;
			subjob.key3 = childId;
			subjob.docId = childId;
			subjob.priority = subprio;
			subjob.dir = nextdir;
			jobManager.insertNew(subjob);
			
			if (Global.debug && nchild>50)
				break;
		}
		childOp.close().throwOnError();
		
		String savefile=null;
		if (nchild==0 && !hasContent)  // foglia
			savefile = job.dir+"/"+job.path;
		else 
			savefile = nextdir+"/"+job.path+" root";
		
		fileh.saveJsonObject("archivi2/"+savefile+".json", node);
		
		if (hasContent && job.withcontent) {
			AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
			subjob.operation = "getArchiviContent2";
			subjob.queue = "qcontent";
			subjob.priority = job.priority + CONTENT_DELTA;
			subjob.dir = savefile;
				
			jobManager.insertNew(subjob);
		}
	}
	
	public void getArchiviContent2(AnasEtlJob job) throws Exception {
		FilenetHelper fnet = caller.getFilenetHelper();
		String id = FilenetDBHelper.dbid2guid(job.docId);
		List<FilenetHelper.ContentInfo> contents = fnet.getContentTransfer(job.os,  id, true);
		for(int i=0;i<contents.size(); i++) {
			FilenetHelper.ContentInfo content = contents.get(i);
			//String filename = i+"."+content.filename;
			String filepath = "archivi2/"+job.dir+" "+content.filename;
			fileHelper.save(filepath, content.stream);	
		}

	}
}
