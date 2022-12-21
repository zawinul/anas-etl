import java.sql.Connection;

import it.eng.anas.Event;
import it.eng.anas.db.FilenetDBConnectionFactory;
import it.eng.anas.db.SimpleDbOp;

public class TestFilenetDB {

	public void test() throws Exception {
		Connection c = FilenetDBConnectionFactory.defaultFactory.getConnection("testDB");
		SimpleDbOp op = new SimpleDbOp(c)
				.query("select count(*) as c from pdm_os.classdefinition")
				.executeQuery()
				.throwOnError();
		op.next();
		int count = op.getInt("c");
		op.close().throwOnError();
		FilenetDBConnectionFactory.close(c);
		System.out.println("c="+count);
		Event.emit("exit");
	}
	
	public static void main(String[] args) throws Exception  {
		new TestFilenetDB().test();
	}

}
