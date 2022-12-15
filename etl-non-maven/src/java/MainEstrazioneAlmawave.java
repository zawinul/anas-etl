

import java.sql.Connection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import it.eng.anas.Event;
import it.eng.anas.FileHelper;
import it.eng.anas.FilenetHelper;
import it.eng.anas.db.FilenetDBConnectionFactory;
import it.eng.anas.db.FilenetDBHelper;
import it.eng.anas.db.SimpleDbOp;

public class MainEstrazioneAlmawave {

	public static void main(String[] args) throws Exception {
		new MainEstrazioneAlmawave().execute();

	}
	

	public void execute() throws Exception {
		executeOsViaAPI("PDM", "PDM_OS", "almawave/api/dbs");
		executeOsViaAPI("PDMASD", "ASD_OS", "almawave/api/archivi");
		executeOsViaDB("PDM", "PDM_OS", "almawave/db/dbs");
		executeOsViaDB("PDMASD", "ASD_OS", "almawave/db/archivi");
		Event.emit("exit");
		System.out.println("bye");
	}


	public void executeOsViaAPI(String os, String schema, String path) throws Exception {
		Connection conn = FilenetDBConnectionFactory.defaultFactory.getConnection("alma");
		FilenetHelper fnh = new FilenetHelper();
		FileHelper fh = new FileHelper();
		new SimpleDbOp(conn)
			.query("alter session set current_schema="+schema)
			.execute()
			.close()
			.throwOnError();
		
		
		SimpleDbOp op1 = new SimpleDbOp(conn)
			.query("select distinct object_class_id from docversion")
			.executeQuery();
		
		while(op1.next()) {
			String cid = op1.getString("object_class_id");
		
			String getADoc = "select  c.symbolic_name as cname, d.object_id "
					+ "from docversion d, classdefinition c "
					+ "where c.object_id=d.object_class_id "
					+ "and c.object_id=? "
					+ "and rownum=1";
			SimpleDbOp op2 = new SimpleDbOp(conn)
				.query(getADoc)
				.setString(1, cid)
				.executeQuery()
				.throwOnError();
			
			op2.next();
			String id = op2.getString("object_id");
			String className = op2.getString("cname");
			op2.close().throwOnError();
			
			ObjectNode n = fnh.getDocumentMetadata(os, id);
			String guid = FilenetDBHelper.dbid2guid(id);
			String dest = path+"/"+className+"."+guid+".json";
			System.out.println("dest = "+dest);
			fh.saveJsonObject(dest, n);
		}
		
		FilenetDBConnectionFactory.close(conn);
	}

	
	public void executeOsViaDB(String os,String schema,  String path) throws Exception {
		Connection conn = FilenetDBConnectionFactory.defaultFactory.getConnection("alma");
		FilenetDBHelper fdb = new FilenetDBHelper("alma"+os, conn);
		FileHelper fh = new FileHelper();
		new SimpleDbOp(conn)
			.query("alter session set current_schema="+schema)
			.execute()
			.close()
			.throwOnError();
		
		
		SimpleDbOp op1 = new SimpleDbOp(conn)
			.query("select distinct object_class_id from docversion")
			.executeQuery();
		
		while(op1.next()) {
			String cid = op1.getString("object_class_id");
		
			String getADoc = "select  c.symbolic_name as cname, d.object_id "
					+ "from docversion d, classdefinition c "
					+ "where c.object_id=d.object_class_id "
					+ "and c.object_id=? "
					+ "and rownum=1";
			SimpleDbOp op2 = new SimpleDbOp(conn)
				.query(getADoc)
				.setString(1, cid)
				.executeQuery()
				.throwOnError();
			
			op2.next();
			String id = op2.getString("object_id");
			String className = op2.getString("cname");
			op2.close().throwOnError();
				
			ArrayNode arr = fdb.getDocProperties(os, id);
			JsonNode obj = arr.get(0);
			String guid = FilenetDBHelper.dbid2guid(id);
			String dest = path+"/"+className+"."+guid+".json";
			System.out.println("dest = "+dest);
			fh.saveJsonObject(dest, obj);
		}
		
		FilenetDBConnectionFactory.close(conn);
	}

}
