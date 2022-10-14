package it.eng.anas.main;

import java.sql.Connection;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.model.DBJob;
import it.eng.anas.model.Esempio1;

public class MainDBSend {

	public static void main(String[] args) throws Exception {
		Connection con = DBConnectionFactory.defaultFactory.getConnection();
		DbJobManager manager = new DbJobManager(con);
		ObjectMapper mapper = Utils.getMapperOneLine();
		for(int i=0; i<500; i++) {
			Esempio1 es = new Esempio1();
			String body = mapper.writeValueAsString(es);
			DBJob job = manager.insertNew("pippoqueue", "{"+Utils.rndString(4)+"}", 10, Utils.rndString(4), body);
			Log.db.log(i+": "+job);
		}
		con.close();
		System.out.println("ok");
	}

}
