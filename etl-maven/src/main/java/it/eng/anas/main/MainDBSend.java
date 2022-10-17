package it.eng.anas.main;

import java.sql.Connection;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.model.DBJob;

public class MainDBSend {

	public static void main(String[] args) throws Exception {
		Connection con = DBConnectionFactory.defaultFactory.getConnection();
		DbJobManager manager = new DbJobManager(con);
		ObjectMapper mapper = Utils.getMapperOneLine();
		for(int i=0; i<500; i++) {
			DBJob job = manager.insertNew(
					"pippo", // queue
					10, // priority
					"get_file", // operation
					Utils.rndString(4), // par1
					Utils.rndString(4), // par2
					Utils.rndString(4), // par3
					Utils.rndString(40) // extra
			);
		}
		con.close();
		System.out.println("ok");
	}

}
