package it.eng.anas.db;

import java.sql.Connection;
import java.util.concurrent.Callable;

import it.eng.anas.Log;

public class DBTransactionManager {
	
	public DBTransactionManager(Connection connection) {
		this.connection = connection;
	}

	public<T> T  execute(Callable<T> b) {
		try {
			T ret = b.call();
			connection.commit();
			return ret;
		}
		catch(Exception e) {
			try {
				Log.db.log(e);
				connection.rollback();
			}
			catch(Exception e2) {
				throw new RuntimeException("errore nella callback in DbJobManager: "+e2.getMessage(), e2);
			}
			throw new RuntimeException("errore in DbJobManager: "+e.getMessage(), e);
		}
	}

	private Connection connection;
	

}
