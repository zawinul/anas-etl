

import java.io.InputStream;
import java.sql.Connection;
import java.util.HashMap;

import org.apache.commons.io.IOUtils;

import it.eng.anas.Event;
import it.eng.anas.Global;
import it.eng.anas.UTF8;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.FilenetDBConnectionFactory;
import it.eng.anas.db.FilenetDBHelper;
import it.eng.anas.db.SimpleDbOp;

public class MainTestOracle {

	private String readSql(String name) throws Exception {
		InputStream stream = MainTestOracle.class.getClassLoader().getResourceAsStream(name+".sql");
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
	
	public void test() throws Exception {
		FilenetDBConnectionFactory f = FilenetDBConnectionFactory.defaultFactory;
		Connection c = f.getConnection("test");
		FilenetDBHelper h = new FilenetDBHelper("aaa",c);
		h.setOs("pdm");;
		HashMap<String, String>map =  h.getDocFieldMap();
		System.out.println(map);
		FilenetDBConnectionFactory.close(c);
		;
	}

	public static void main(String[] args) throws Exception {
		Global.args = args;
		new MainTestOracle().test();
		System.out.println("ok");
	}
}
