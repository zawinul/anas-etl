

import java.io.InputStream;
import java.sql.Connection;

import org.apache.commons.io.IOUtils;

import it.eng.anas.Event;
import it.eng.anas.Global;
import it.eng.anas.UTF8;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.SimpleDbOp;

public class MainInitDB {

	private String readSql(String name) throws Exception {
		InputStream stream = MainInitDB.class.getClassLoader().getResourceAsStream(name+".sql");
		String sql = IOUtils.toString(stream, UTF8.charset);
		return sql;
	}
	
	private void exec(String sql, Connection con) throws Exception {
		System.out.println(sql);
		new SimpleDbOp(con)
			.query(sql)
			.execute()
			.close()
			.throwOnError();
		System.out.println("done!");
	}
	
	public void createDb() throws Exception {
		Connection con = DBConnectionFactory.defaultFactory.getConnection("initDB");
		
		String tablesToDrop[] = {"job", "job_done", "job_error", "jobid_sequence"};
		for(String table: tablesToDrop) 
			exec("DROP TABLE IF EXISTS "+table, con);
		
		String sql = readSql("create-job-table");
		String tablesToCreate[] = {"job", "job_done", "job_error"};
		for(String table:tablesToCreate) 
			exec(sql.replace("tablename", table), con);
		
		exec(readSql("create-sequence"), con);
		exec(readSql("init-sequence"), con);
		DBConnectionFactory.close(con);
		Event.emit("exit");
	}

	public static void main(String[] args) throws Exception {
		Global.args = args;
		new MainInitDB().createDb();
		System.out.println("ok");
	}
}
