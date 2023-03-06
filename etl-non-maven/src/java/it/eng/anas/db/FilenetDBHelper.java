package it.eng.anas.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import it.eng.anas.FilenetHelper;
import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.model.Config;

public class FilenetDBHelper {
//alter session set current_schema=pdm_os;

	public FilenetDBHelper(String tag, Connection conn) {
		super();
		this.tag = tag;
		this._conn = conn;
	}

	public FilenetDBHelper(String tag) {
		super();
		this.tag = tag;
		this._conn = null;
	}

	private String tag;
	private Connection _conn = null;

	public Connection getConnection() throws Exception {
		if (_conn == null)
			_conn = FilenetDBConnectionFactory.defaultFactory.getConnection(tag);
		return _conn;
	}
	
	private String os = null;
	public void setOs(String os) throws Exception {
		if (!os.equals(this.os)) {
			this.os = os;
			String schema = os;
			if (os.toLowerCase().equals("pdm"))
				schema = "PDM_OS";
			if (os.toLowerCase().equals("pdmasd"))
				schema = "ASD_OS";
			new SimpleDbOp(getConnection())
			.query("alter session set current_schema="+schema)
			.execute()
			.close()
			.throwOnError();
		}
	}
	
	public List<String[]> getSubFolders(String os, String folderId, String path) throws Exception {
		if (folderId.startsWith("{"))
			folderId = guid2dbid(folderId);
		setOs(os);
		SimpleDbOp op = new SimpleDbOp(getConnection())
			.query("select object_id, name from container where parent_container_id=?")
			.setString(1,  folderId)
			.executeQuery()
			.throwOnError();

		List<String []> ret = new ArrayList<String[]>();
		while(op.next()) {
			ret.add(new String[] {
					op.getString("object_id"),
					path+"/"+op.getString("name")
			});
		}
		op.close();
		op.throwOnError();
		return ret;
	}
	
	public abstract interface FolderListener {
		public void onFolder(String folderId, String path) throws Exception;
	}
	
	public void getSubFolders(String os, String folderId, String path, FolderListener listener) throws Exception {
		if (folderId.startsWith("{"))
			folderId = guid2dbid(folderId);
		setOs(os);
		SimpleDbOp op = new SimpleDbOp(getConnection())
			.query("select object_id, name from container where parent_container_id=? order by name")
			.setString(1,  folderId)
			.executeQuery()
			.throwOnError();

		while(op.next()) 
			listener.onFolder(op.getString("object_id"), path+"/"+op.getString("name"));
		op.close();
		op.throwOnError();
	}

	public abstract interface DocIdListener {
		public void onDoc(String docId) throws Exception;
	}
	
	public static class Version {
		public String id;
		public String vid;
		public long created;
	}
	public void getContainedDocumentsId(String os, String folderId, DocIdListener listener) throws Exception {
		if (folderId.startsWith("{"))
			folderId = guid2dbid(folderId);

		String getDocQuery = "select d.object_id, d.version_series_id, d.create_date " +
				"from docversion d where d.retrieval_names is not null and d.object_id in "+ 
				" (select head_id from relationship  where tail_id=? and object_class_id=?) order by d.content_size desc";
		
		String  t0s = Utils.getConfig().snapshotDate;
		long t0 = Utils.string2Date(t0s).getTime();
		
		setOs(os);
		SimpleDbOp op = new SimpleDbOp(getConnection())
			.query(getDocQuery)
			.setString(1,  folderId)
			.setString(2, getFolderRelationshipId(os))
			.executeQuery()
			.throwOnError();

		Map<String, ArrayList<Version>> map = new HashMap<>();
		while(op.next()) {
			Version v = new Version();
			v.id = op.getString("object_id");
			v.vid = op.getString("version_series_id");
			v.created = op.getDate("create_date").getTime();
			if (v.created>t0) {
				Log.log("versione "+dbid2guid(v.id) +" creata in data "+new Date(v.created)+" : scartata");
				continue;
			}
			if (!map.containsKey(v.vid))
				map.put(v.vid,  new ArrayList<Version>());
			map.get(v.vid).add(v);
		}
		op.close().throwOnError();
		
		if (map.isEmpty())
			return;
		
		for (String vid: map.keySet()) {
			ArrayList<Version> versions = map.get(vid);

			// poichè le versioni troppo recenti le ho già scartate la più recente tra quelle rimaste è quella che mi interessa

			Version selected = versions.get(0);
			if (versions.size()>=2) {
				Log.log("caso da debug vid="+vid+" n="+versions.size());
				for(int i=1;i<versions.size();i++) {
					Version candidate = versions.get(i);
					if (candidate.created>selected.created) {
						Log.log(Utils.time2String(selected.created)+" rimpiazzato da "+Utils.time2String(candidate.created));
						selected = candidate;
					}
				}		
				Log.log("selected="+Utils.time2String(selected.created));
			}

			listener.onDoc(selected.id);
		}
	}
	
	public static String guid2dbid(String guid) {
		if (!guid.startsWith("{"))
			return guid;
		String s1 = guid.substring(1, 3);
		String s2 = guid.substring(3, 5);
		String s3 = guid.substring(5, 7);
		String s4 = guid.substring(7, 9);
		String s5 = guid.substring(10, 12);
		String s6 = guid.substring(12, 14);
		String s7 = guid.substring(15, 17);
		String s8 = guid.substring(17, 19);
		String s9 = guid.substring(20, 24);
		String s10 = guid.substring(25, 37);
		return s4 + s3 + s2 + s1 + s6 + s5 + s8 + s7 + s9 + s10;

	}

	public static String dbid2guid(String dbid) {
		if (dbid.startsWith("{"))
			return dbid;
		String s1 = dbid.substring(0, 2);
		String s2 = dbid.substring(2, 4);
		String s3 = dbid.substring(4, 6);
		String s4 = dbid.substring(6, 8);
		String s5 = dbid.substring(8, 10);
		String s6 = dbid.substring(10, 12);
		String s7 = dbid.substring(12, 14);
		String s8 = dbid.substring(14, 16);
		String s9 = dbid.substring(16, 20);
		String s10 = dbid.substring(20);
		return "{" + s4 + s3 + s2 + s1 + "-" + s6 + s5 + "-" + s8 + s7 + "-" + s9 + "-" + s10 + "}";
	}

	public static void main(String args[]) throws Exception {
		String guid = "{469D4480-EFC4-4B95-A23E-43D67E29AF71}";
		String db = guid2dbid(guid);
		System.out.println("db=" + db);
		String wp2 = dbid2guid(db);
		System.out.println("wp2=" + wp2);
		
	}

	public static void main2(String args[]) throws Exception {
		FilenetDBHelper h = new FilenetDBHelper("prova");
		Config cfg = Utils.getConfig();
		h.setOs("pdm");
		List<String[]> sf = h.getSubFolders("pdm", cfg.idProgetti, "pippo");
		System.out.println(sf.size());
		for(String[] row: sf)
			System.out.println(Utils.getMapper().writeValueAsString(row));
	}
	
	private String _frId=null;
	private String getFolderRelationshipId(String os) throws Exception{
		setOs(os);
		if (_frId==null) {
			SimpleDbOp op = new SimpleDbOp(getConnection())
				.query("select object_id from classdefinition where symbolic_name=?")
				.setString(1, "ReferentialContainmentRelationship")
				.executeQuery();
			op.next();
			_frId = op.getString("object_id");
			op.close().throwOnError();
		}
		return _frId;
	}
	

	private HashMap<String, String> _docfieldmap = null;
	public  HashMap<String, String> getDocFieldMap() throws Exception {
		if (_docfieldmap==null) {
			_docfieldmap = new HashMap<String, String>();
			SimpleDbOp op = new SimpleDbOp(getConnection())
			.query("select c.column_name,g.symbolic_name from columndefinition c, globalpropertydef g "
					+ " where c.dbg_table_name='DocVersion' and c.prop_id=g.object_id")
			.executeQuery()
			.throwOnError();
			
			while(op.next()) {
				String colname = op.getString("column_name");
				String name =  op.getString("symbolic_name");
				_docfieldmap.put(colname, name);
			}
			
			op.close().throwOnError();
		}
		return _docfieldmap;
	}

	
	public ObjectNode getDocProperties(String os, String id) throws Exception {
		setOs(os);
		if (id.startsWith("{"))
			id = guid2dbid(id);
		
		SimpleDbOp op = new SimpleDbOp(getConnection())
			.query("select * from docversion where object_id=?")
			.setString(1, id)
			.executeQuery()
			.throwOnError();
		
		ArrayNode an = null;
		HashMap<String, String> map = getDocFieldMap();
		ResultSet rs = op.getResultSet();
		ResultSetToJson r = new ResultSetToJson();
		an = r.extractWithTypes(rs, map);
		ObjectNode obj = (ObjectNode) an.get(0);
		op.close().throwOnError();

		return obj;

	}

}
