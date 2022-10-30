package it.eng.anas.main;

import java.sql.Connection;
import it.eng.anas.Utils;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.DbJobManager;

public class MainDBSend {

	public static void main(String[] args) throws Exception {
		Connection con = DBConnectionFactory.defaultFactory.getConnection("dbsendMain");
		DbJobManager manager = new DbJobManager(con);
		for(int i=0; i<5000; i++) {
			manager.insertNew(
					"anas-etl", // queue
					10, // priority
					"get_file", // operation
					Utils.rndString(4), // par1
					Utils.rndString(4), // par2
					Utils.rndString(4), // par3
					-1, //parentJob,
					null // extra
			);
		}
		DBConnectionFactory.close(con);
		System.out.println("ok");
	}

}
