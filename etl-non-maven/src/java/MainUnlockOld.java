

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;

import it.eng.anas.Event;
import it.eng.anas.Global;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.SimpleDbOp;

public class MainUnlockOld {

	private void exec(String sql, Connection con) throws Exception {
		System.out.println(sql);
		new SimpleDbOp(con)
			.query(sql)
			.execute()
			.close()
			.throwOnError();
		System.out.println("done!");
	}

	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssS");

	public void clearDB() throws Exception {
		Connection con = DBConnectionFactory.defaultFactory.getConnection("unlock-old");
		long now = new Date().getTime();
		long expired = now-30*60*1000;
		Date expdate = new Date(expired);
		String formatted = dateFormat.format(expdate);
		
		System.out.println("un-lock all operation locked before "+expdate);
		String sql = "update job set locktag=null where locktag is not null and last_change<?";
		SimpleDbOp op = new SimpleDbOp(con)
			.query(sql)
			.setString(1, formatted)
			.executeUpdate()
			.throwOnError();
		System.out.println("#updated = "+op.getNumOfExecutedUpdate());
		op.close();
		DBConnectionFactory.close(con);
		Event.emit("exit");
	}

	public static void main(String[] args) throws Exception {
		Global.args = args;
		new MainUnlockOld().clearDB();
		System.out.println("ok");
	}
}
