package it.eng.anas.etl;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

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

public class JobProcessorArchivi {
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
 	
	public JobProcessorArchivi(AnasEtlWorker caller) {
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
			savefile = nextdir+"/"+job.path;
		
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
	
	private static class SavedJob {
		public AnasEtlJob job;
		public String titolo;
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
		
		List<SavedJob> subjobs = new ArrayList<>();
		boolean costruzioniFound=false;
		while(iter.hasNext()) {
			RepositoryRow row = (RepositoryRow) iter.next();
			String childId = row.getProperties().getIdValue("Id").toString();
			Integer childNumero = row.getProperties().getInteger32Value("numero");
			String childTitolo = row.getProperties().getStringValue("titolo").trim().toLowerCase();
			int subprio = job.priority-1;
//			if (level==2) {
//				if (!titolo.trim().toLowerCase().equals("nuove costruzioni"))
//					subprio-= 10000;
//				else
//					subprio -=1000;
//			}
//			if (level>2)
//				subprio -= 1;
			
			if (childTitolo.equals("nuove costruzioni")) {
				costruzioniFound = true;
			}
			String nextpath = job.path+"."+childNumero;
			AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
			subjob.operation = "getArchiviNode";
			subjob.queue = "qdata";
			subjob.path = nextpath;
			
			subjob.key1 = level==1 ? nextdir : job.key1; // compartimento
			//subjob.key2 = level==2 ? nextdir : job.key2; // compartimento/subfondo
			subjob.key2 = job.key2;
			subjob.key3 = nextpath; // sequenza numeri

			
			subjob.docId = childId;
			subjob.priority = subprio;
			subjob.dir = nextdir;
			SavedJob sj = new SavedJob();
			sj.titolo = childTitolo;
			sj.job = subjob;
			subjobs.add(sj);
			nchild++;
//			
//			if (Global.debug && nchild>50)
//				break;

		}
		if (costruzioniFound) {
			for(SavedJob sjob:subjobs) {
				if (!sjob.titolo.equals("nuove costruzioni"))
					sjob.job.priority-=1000;
				else
					sjob.job.priority-=10000;
				sjob.job.key2 = sjob.titolo;
			}
		}
		
		for(SavedJob sjob:subjobs)
			jobManager.insertNew(sjob.job);
		

		
		String savefile=null;
		if (nchild==0 && !hasContent)  // foglia
			savefile = job.dir+"/"+job.path;
		else 
			savefile = nextdir+"/"+job.path;
		
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
		

	
	public void getArchiviNode2ByApi(AnasEtlJob job) throws Exception {
		String id = job.docId;		
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
		FilenetHelper fnet = caller.getFilenetHelper();
		int level = job.path.split("\\.").length;
		ObjectNode node = fnet.getDocumentMetadata("PDMASD", id);
		if (node==null)
			throw new Exception("doc not found "+id);

		int numero = node.get("numero").asInt();
		String titolo = node.get("titolo").asText();
		String	nextdir = job.dir+"/"+numero;
		boolean hasContent = false;
		JsonNode jsize = node.get("ContentSize");
		if (jsize!=null)
			hasContent = jsize.asLong()>0;
		
		int nchild=0;
		SearchSQL sql = new SearchSQL();
		sql.setMaxRecords(10000);
		sql.setSelectList("d.Id,d.codiceIdentificativoArchivio,d.numero,d.titolo");
		sql.setFromClauseInitialValue("AsdElementoArchivistico", "d", true);
		String whereClause = "d.elementoArchivisticoParentRef="+id;
		sql.setWhereClause(whereClause);

		ObjectStore os = fnet.getOS("PDMASD");
		SearchScope searchScope = new SearchScope(os);
		RepositoryRowSet rowSet = searchScope.fetchRows(sql, 10000, null, true);
		Iterator iter = rowSet.iterator();		
		while(iter.hasNext()) {
			RepositoryRow row = (RepositoryRow) iter.next();
			String childId = row.getProperties().getIdValue("Id").toString();
			Integer childNumero = row.getProperties().getInteger32Value("numero");
			String childTitolo = row.getProperties().getStringValue("titolo").trim().toLowerCase();
			int subprio = job.priority-1;
			String nextpath = job.path+"."+childNumero;
			Log.log(nextpath+" "+childTitolo);
			AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
			subjob.operation = "getArchiviNode2";
			subjob.queue = "qdata";
			subjob.path = nextpath;
			subjob.key3 = nextpath; 
			
			subjob.docId = childId;
			subjob.priority = subprio;
			subjob.dir = nextdir;
			jobManager.insertNew(subjob);
			nchild++;
		}
		String savefile=null;
		if (nchild==0 && !hasContent)  // foglia
			savefile = job.dir+"/"+job.path;
		else 
			savefile = nextdir+"/"+job.path;
		
		fileh.saveJsonObject("archivi2/"+savefile+".json", node);
		
		if (hasContent && job.withcontent) {
			AnasEtlJob contentjob = AnasEtlJob.createSubJob(job);
			contentjob.operation = "getArchiviContent2";
			contentjob.queue = "qcontent";
			contentjob.priority = job.priority + CONTENT_DELTA;
			contentjob.dir = savefile;
				
			jobManager.insertNew(contentjob);
		}
	}

	
	
	
	public void getArchiviContent2(AnasEtlJob job) throws Exception {
		FilenetHelper fnet = caller.getFilenetHelper();
		String id = FilenetDBHelper.dbid2guid(job.docId);
		List<FilenetHelper.ContentInfo> contents = fnet.getContentTransfer("PDMASD",  id, true);
		for(int i=0;i<contents.size(); i++) {
			FilenetHelper.ContentInfo content = contents.get(i);
			//String filename = i+"."+content.filename;
			String filepath = "archivi2/"+job.dir+" "+content.filename;
			Log.log("save "+filepath);
			fileHelper.save(filepath, content.stream);	
		}
	}
	
	public void startArchiviNode(AnasEtlJob job) throws Exception {
		String nodeId = findNodeId(job.path);
		if (nodeId==null)
			throw new Exception("path not found: "+job.path);
		DbJobManager<AnasEtlJob> jobManager = caller.getJobManager();
		String parts[] = job.path.split("\\.");

		AnasEtlJob subjob = AnasEtlJob.createSubJob(job);
		subjob.operation = "getArchiviNode2";
		subjob.docId = nodeId;
		subjob.key1 = parts[0];
		subjob.key2 = job.path;
		subjob.key3 = job.path;
		jobManager.insertNew(subjob);
	}
 	
	public String findNodeId(String path) throws Exception {
		String parts[] = path.split("\\.");

		String id=null;
		FilenetHelper fnet = caller.getFilenetHelper();
		ObjectStore os = fnet.getOS("PDMASD");

		SearchSQL mainsql = new SearchSQL();
		//mainsql.setMaxRecords(1);
		String select = "d.Id,d.codiceIdentificativoArchivio,d.numero";
		mainsql.setSelectList(select);
		String myClassName1 = "AsdElementoArchivistico";
		String myAlias1 = "d";
		boolean subclassesToo = true;
		mainsql.setFromClauseInitialValue(myClassName1, myAlias1, subclassesToo);
		String whereClause = "d.elementoArchivisticoParentRef IS NULL and d.codiceIdentificativoArchivio='"+parts[0]+"'";
		mainsql.setWhereClause(whereClause);

		Log.log("SQL: " + mainsql.toString());
		SearchScope searchScope = new SearchScope(os);
		RepositoryRowSet rowSet = searchScope.fetchRows(mainsql, null, null, true);
		Iterator iter = rowSet.iterator();
		while(iter.hasNext()) {
			RepositoryRow row = (RepositoryRow) iter.next();
			id = row.getProperties().getIdValue("Id").toString();
			String arch = row.getProperties().getStringValue("codiceIdentificativoArchivio");
			Log.log("arch="+arch);
			break;
		}
		for(int i=1; i<parts.length;i++) {
			int numero = Integer.parseInt(parts[i]);
			SearchSQL sql = new SearchSQL();
			//sql.setMaxRecords(1);
			String sel2 = "d.Id,d.numero,d.elementoArchivisticoParentRef";
			sql.setSelectList(sel2);
			sql.setFromClauseInitialValue(myClassName1, myAlias1, subclassesToo);
			//String whereClause2 = "d.numero="+numero+ " AND d.elementoArchivisticoParentRef="+id;
			String whereClause2 = "d.numero="+numero+" AND d.elementoArchivisticoParentRef="+id;
			sql.setWhereClause(whereClause2);

			Log.log("SQL: " + sql.toString());
			SearchScope searchScope2 = new SearchScope(os);
			RepositoryRowSet rowSet2 = searchScope2.fetchRows(sql, null, null, true);
			Iterator iter2 = rowSet2.iterator();
			String parentId = id;
			id = null;
			while(iter2.hasNext()) {
				RepositoryRow row = (RepositoryRow) iter2.next();
				int n = row.getProperties().getInteger32Value("numero");
				String parent = row.getProperties().getIdValue("elementoArchivisticoParentRef").toString();
				Log.log("n"+n+" parent="+parent);
				if (n==numero && parent.equals(parentId)) {
					id = row.getProperties().getIdValue("Id").toString();
					break;
				}
				break;
			}
		}
		return id;
	}
	
	public static class ArchChild {
		public String id;
		public String path;
		public String titolo;
		public int numero;
	}
	
	public List<ArchChild> getArchChild(String path) throws Exception {
		String id = findNodeId(path);
		if (id==null)
			return null;

		List<ArchChild> ret = new ArrayList<>();
		SearchSQL sql = new SearchSQL();
		sql.setMaxRecords(10000);
		sql.setSelectList("d.Id,d.codiceIdentificativoArchivio,d.numero,d.titolo");
		sql.setFromClauseInitialValue("AsdElementoArchivistico", "d", true);
		String whereClause = "d.elementoArchivisticoParentRef="+id;
		sql.setWhereClause(whereClause);

		FilenetHelper fnet = caller.getFilenetHelper();
		ObjectStore os = fnet.getOS("PDMASD");
		SearchScope searchScope = new SearchScope(os);
		RepositoryRowSet rowSet = searchScope.fetchRows(sql, 10000, null, true);
		Iterator iter = rowSet.iterator();		
		while(iter.hasNext()) {
			RepositoryRow row = (RepositoryRow) iter.next();
			ArchChild c = new ArchChild();
			c.id = row.getProperties().getIdValue("Id").toString();
			c.numero = row.getProperties().getInteger32Value("numero");;
			c.path = path+"."+c.numero;
			c.titolo = row.getProperties().getStringValue("titolo").trim().toLowerCase();
			ret.add(c);
		}	
		return ret;
	}
	
	
	public static void main(String args[]) throws Exception {
		//final String PATH = "BA.3";
		AnasEtlWorker worker = new AnasEtlWorker("test");
		JobProcessorArchivi proc = new JobProcessorArchivi(worker);
		Scanner sc = new Scanner(System.in);
		while(true) {
			System.out.println("path ? ");
			String line = sc.nextLine();
			System.out.println("line=["+line+"]");
			line = line.trim();
			if (line.equals("exit"))
				break;
			List<ArchChild> children = proc.getArchChild(line);
			int i=0;
			
			children.sort(new Comparator<ArchChild>() {
				public int compare(ArchChild o1, ArchChild o2) {
					return o1.numero-o2.numero;
				}
			});
			for(ArchChild child: children) {
				System.out.format("%3d) %16s %s %s\n", i++, child.path, child.id, child.titolo);
			}
		}
		System.out.println("exiting");
		Event.emit("exit");
		Log.log("done!");
	}
}
