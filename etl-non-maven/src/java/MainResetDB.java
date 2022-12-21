

import java.io.InputStream;
import java.sql.Connection;

import org.apache.commons.io.IOUtils;

import it.eng.anas.Event;
import it.eng.anas.Global;
import it.eng.anas.UTF8;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.SimpleDbOp;

public class MainResetDB {

	private void exec(String sql, Connection con) throws Exception {
		System.out.println(sql);
		new SimpleDbOp(con)
			.query(sql)
			.execute()
			.close()
			.throwOnError();
		System.out.println("done!");
	}
	
	public void clearDB() throws Exception {
		Connection con = DBConnectionFactory.defaultFactory.getConnection("initDB");
		
		String tablesToTruncate[] = {"job", "job_done", "job_error"};
		for(String table: tablesToTruncate) 
			exec("TRUNCATE TABLE  "+table, con);
		
		exec("update jobid_sequence set id=0", con);
		
		DBConnectionFactory.close(con);
		Event.emit("exit");
	}

	public static void main(String[] args) throws Exception {
		Global.args = args;
		new MainResetDB().clearDB();
		System.out.println("ok");
	}
}
