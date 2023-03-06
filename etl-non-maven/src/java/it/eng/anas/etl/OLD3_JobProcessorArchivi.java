package it.eng.anas.etl;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.filenet.api.collection.RepositoryRowSet;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.query.RepositoryRow;
import com.filenet.api.query.SearchSQL;
import com.filenet.api.query.SearchScope;

import it.eng.anas.Event;
import it.eng.anas.FileHelper;
import it.eng.anas.FilenetHelper;
import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.db.FilenetDBHelper;
import it.eng.anas.db.ResultSetToJson;
import it.eng.anas.db.SimpleDbOp;

public class OLD3_JobProcessorArchivi {
	public Connection fnconn;
	public FilenetDBHelper db;
	public FileHelper fileh = new FileHelper();
	
	protected AnasEtlWorker caller;
	protected FileHelper fileHelper = new FileHelper();
	

	
	// CHILD_DELTA>0 => esplora in profondità 
	// CHILD_DELTA<0 => esplora in ampiezza
	public static int CHILD_DELTA = -1; 

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
 	
	public OLD3_JobProcessorArchivi(AnasEtlWorker caller) {
		this.caller = caller;
		try {
			if (Utils.getConfig().directFilenetDbAccess) {
				db = caller.getDB();
				db.setOs("pdmasd");
				fnconn = db.getConnection();
				getDocFieldMap();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public void startScanArchivi2(AnasEtlJob job) throws Exception {
		if (!Utils.getConfig().directFilenetDbAccess) {
			startScanArchiviByAPI(job);
			return;
		}
		String query="select object_id, u74_numero, U62_CODICEIDENTIFICATIVOARCHIV from docversion "
				+ "where U75_ELEMENTOARCHIVISTICOPARENT is null "
				+ "and U62_CODICEIDENTIFICATIVOARCHIV is not null "
				+ "and u76_titolo is not null";

		SimpleDbOp op1 = new SimpleDbOp(fnconn)
			.query(query)
			.executeQuery()
			.throwOnError();
		
		
		List<AnasEtlJob> compartimentiJob = new ArrayList<>();
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
			compartimentiJob.add(subjob);
		}
		op1.close().throwOnError();
		
		compartimentiJob.sort(new Comparator<AnasEtlJob>() {
			public int compare(AnasEtlJob o1, AnasEtlJob o2) {
				return (o1.path.compareTo(o2.path));
			}
		});
		for(int j=0; j<compartimentiJob.size(); j++) {
			AnasEtlJob subjob = compartimentiJob.get(j);
			subjob.priority = -100*j;
			jobManager.insertNew(subjob);			
		}
		fileh.saveJsonObject("archivi2/compartimenti-job", compartimentiJob);

	}
	

	public void getArchiviNode(AnasEtlJob job) throws Exception {
		if (!Utils.getConfig().directFilenetDbAccess ) {
			getArchiviNodeByApi(job);
			return;
		}
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
			String childTitolo = childOp.getString("u76_titolo");
			int subprio = job.priority+CHILD_DELTA;
			
//			if (!titolo.trim().toLowerCase().equals("nuove costruzioni"))
//				subprio-=10000;
			if (level==1 && !childTitolo.trim().toLowerCase().equals("nuove costruzioni"))
				continue;
			
			// esempio
			if (childTitolo.trim().toLowerCase().equals("ancona"))
				subprio+=200;
			if (childTitolo.trim().toLowerCase().equals("perugia"))
				subprio-=200;
			
			String nextpath = job.path+"."+numero;
			AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
			subjob.operation = "getArchiviNode";
			subjob.queue = "qdata";
			subjob.path = nextpath;
			subjob.key1 = level==2 ? nextdir : job.key2;
			subjob.key2 = nextpath;
			subjob.key3 = childId;
			subjob.docId = childId;
			subjob.priority = subprio;
			subjob.dir = nextdir;
			jobManager.insertNew(subjob);
			
//			if (Global.debug && nchild>50)
//				break;
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
	


	
	public void startScanArchiviByAPI(AnasEtlJob job) throws Exception {
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
		FilenetHelper fnet = caller.getFilenetHelper();
		ObjectStore os = fnet.getOS("PDMASD");

		SearchSQL sql = new SearchSQL();
		sql.setMaxRecords(10000);

		// Specify the SELECT list using the setSelectList method.
		String select = "d.Id,d.codiceIdentificativoArchivio,d.numero";
		sql.setSelectList(select);

		String myClassName1 = "AsdElementoArchivistico";
		String myAlias1 = "d";
		boolean subclassesToo = false;
		sql.setFromClauseInitialValue(myClassName1, myAlias1, subclassesToo);

		String whereClause = "d.elementoArchivisticoParentRef IS NULL and d.codiceIdentificativoArchivio IS NOT NULL";
		sql.setWhereClause(whereClause);

		Log.log("SQL: " + sql.toString());

		SearchScope searchScope = new SearchScope(os);
		RepositoryRowSet rowSet = searchScope.fetchRows(sql, 10000, null, true);
		Iterator iter = rowSet.iterator();
		int i=0;
		List<AnasEtlJob> compartimentiJob = new ArrayList<>();
		while(iter.hasNext()) {
			RepositoryRow row = (RepositoryRow) iter.next();
			String id = row.getProperties().getIdValue("Id").toString();
			String arch = row.getProperties().getStringValue("codiceIdentificativoArchivio");
			Integer numero = row.getProperties().getInteger32Value("numero");
			Log.log(i+") "+arch+" n="+numero+" id="+id);
			
			AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
			subjob.operation = "getArchiviNode";
			subjob.queue = "qdata";
			subjob.path = arch;
			subjob.key1 = "archivi2";
			subjob.key2 = arch;
			subjob.key3 = id;
			subjob.priority = -i*20;
			subjob.os = "PDMASD";
			subjob.docId = id;
			subjob.dir = null;
			compartimentiJob.add(subjob);
			i++;
		}
		compartimentiJob.sort(new Comparator<AnasEtlJob>() {
			public int compare(AnasEtlJob o1, AnasEtlJob o2) {
				return (o1.path.compareTo(o2.path));
			}
		});
		for(int j=0; j<compartimentiJob.size(); j++) {
			AnasEtlJob subjob = compartimentiJob.get(j);
			subjob.priority = -10*j;
			jobManager.insertNew(subjob);			
		}
		fileh.saveJsonObject("archivi2/compartimenti-job.json", compartimentiJob);
		
	}
	public void getArchiviNodeByApi(AnasEtlJob job) throws Exception {
		String id = job.docId;
		
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
		FilenetHelper fnet = caller.getFilenetHelper();
		int level = job.path.split("\\.").length;

		ObjectNode node = fnet.getDocumentMetadata("PDMASD", id);
		
		if (node==null)
			throw new Exception("doc not found "+id);

		String nextdir=null;
		int numero = node.get("numero").asInt();
		String titolo = node.get("titolo").asText();
		Log.log("\t"+job.path+" "+"\ttitolo="+titolo);
		if (level==1) 
			nextdir=job.path;
		else if (level==2) {
			nextdir = job.dir+"/"+numero+" "+titolo;
			Log.log("nextdir="+nextdir);
		}
		else
			nextdir = job.dir+"/"+numero;
		boolean hasContent = false;
		JsonNode jsize = node.get("ContentSize");
		if (jsize!=null)
			hasContent = jsize.asLong()>0;
		
		SearchSQL sql = new SearchSQL();
		sql.setMaxRecords(10000);
		String select = "d.Id,d.codiceIdentificativoArchivio,d.numero,d.titolo";
		sql.setSelectList(select);

		boolean subclassesToo = true;
		sql.setFromClauseInitialValue("AsdElementoArchivistico", "d", subclassesToo);

		String whereClause = "d.elementoArchivisticoParentRef="+id;
		sql.setWhereClause(whereClause);

		int nchild=0;
		ObjectStore os = fnet.getOS("PDMASD");
		SearchScope searchScope = new SearchScope(os);
		// Uses fetchRows to test the SQL statement.
		RepositoryRowSet rowSet = searchScope.fetchRows(sql, 10000, null, true);
		Iterator iter = rowSet.iterator();
		while(iter.hasNext()) {
			RepositoryRow row = (RepositoryRow) iter.next();
			String childId = row.getProperties().getIdValue("Id").toString();
			Integer childNumero = row.getProperties().getInteger32Value("numero");
			// String childTitolo = row.getProperties().getStringValue("titolo");
			int subprio = job.priority;
			if (level==2) {
				if (!titolo.trim().toLowerCase().equals("nuove costruzioni"))
					subprio-= 10000;
				else
					subprio -=1000;
			}
			if (level>2)
				subprio -= 1;
			

			
			String nextpath = job.path+"."+childNumero;
			AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
			subjob.operation = "getArchiviNode";
			subjob.queue = "qdata";
			subjob.path = nextpath;
			
			subjob.key1 = level==1 ? nextdir : job.key1; // compartimento
			subjob.key2 = level==2 ? nextdir : job.key2; // compartimento/subfondo
			subjob.key3 = nextpath; // sequenza numeri

			
			subjob.docId = childId;
			subjob.priority = subprio;
			subjob.dir = nextdir;
			jobManager.insertNew(subjob);
			nchild++;
//			
//			if (Global.debug && nchild>50)
//				break;

		}
		

		
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
	
	public static void main(String args[]) throws Exception {
		AnasEtlWorker worker = new AnasEtlWorker("test");
		OLD3_JobProcessorArchivi proc = new OLD3_JobProcessorArchivi(worker);
		proc.startScanArchiviByAPI(null);
		Event.emit("exit");
		Log.log("done!");
	}
}
