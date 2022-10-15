package it.eng.anas.db;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;

import it.eng.anas.Log;
import it.eng.anas.UTF8;




public class SimpleDbOp implements Cleanable {
	private String sql;
	private Connection conn = null;
	private Connection localConnection;
	private PreparedStatement ps = null;
	private ResultSet rs = null;
	private boolean error = false;
	private Exception lastException = null;
	private boolean closed = false;
	private int nExecutedUpdate = -1;
	private static DBConnectionFactory factory = DBConnectionFactory.defaultFactory;
	
	
	public static void setConnectionFactory(DBConnectionFactory fact) {
		factory = fact;
	}
	
	public SimpleDbOp() {
		this(null, true);
	}


	public SimpleDbOp(Connection con) {
		this(con, true);
	}
	
	
	public SimpleDbOp(Connection passedConnection, boolean inibisciEccezioni) {
		this.inibisciEccezioni = inibisciEccezioni;

		if (passedConnection!=null) {
			conn = passedConnection;
		}
		else {
			conn = localConnection = getConnection();
		}
		if (error)
			return;

		final SimpleDbOp t = this;
		Cleaner cleaner = Cleaner.create();
		cleaner.register(this, new Runnable() {
			public void run() {
				t.clean();
			}
		});
	}
	
	public SimpleDbOp query(String sql) {
		this.sql = sql;
		try {
			ps = conn.prepareStatement(sql);
		} catch (Exception e) {
			error = true;
			lastException = e;
		}
		return this;
	}
	
	protected Connection getConnection() {
		if (error)
			return null;
		
		try {
			if (conn==null)
				conn = factory.getConnection();
		} catch (Exception e) {
			error = true;
			lastException = e;
		}
		return conn;
	}

	public SimpleDbOp close() {
		if (!closed) {
			closeAll(localConnection, ps, rs);
			conn = null;
			localConnection = null;
			ps=null;
			rs=null;
			closed = true;
		}
		return this;		
	}
	
	public  void closeAll(Object... x) {
		// chiude prima tutti i resultset, poi gli statement, poi le connection
		for(Object ob:x) {
			if (ob != null && ob instanceof ResultSet)
				try{ ((ResultSet) ob).close();}catch(Exception e){}
		}
		for(Object ob:x) {
			if (ob != null && ob instanceof Statement)
				try{ ((Statement) ob).close();}catch(Exception e){}
			if (ob != null && ob instanceof PreparedStatement)
				try{ ((PreparedStatement) ob).close();}catch(Exception e){}
		}
		for(Object ob:x) {
			if (ob != null && ob instanceof Connection) {
				String shortsql = sql.length()>50 ? sql.substring(0,50)+"..." : sql;
				Log.db.log("chiudo la connection "+ob +" "+ shortsql);
				try{ ((Connection) ob).close();}catch(Exception e){}
			}
		}

	}

	public boolean isOk() {
		return !error;
	}
	
	public SimpleDbOp execute() {
		try {
			if (!error)
				ps.execute();
		}
		catch (Exception e) {
			gestisciEccezione(e);
		}
		return this;
	}
	
	public SimpleDbOp executeUpdate() {
		try {
			if (!error)
				nExecutedUpdate = ps.executeUpdate();
		}
		catch (Exception e) {
			gestisciEccezione(e);
		}
		return this;
	}
	
	public int getNumOfExecutedUpdate() {
		return nExecutedUpdate;
	}

	public boolean next() {
		if (error)
			return false;
		
		if (rs==null) {
			gestisciEccezione("chiamata a next() senza il resultset, sql="+sql);
			return false;
		}
		try {
			return rs.next();
		} 
		catch (SQLException e) {
			gestisciEccezione("errore chiamata a next, sql="+sql, e);
			return false;
		}
	}
	
	public SimpleDbOp executeQuery() {
		try {
			if (!error)
				rs = ps.executeQuery();
		} 
		catch (Exception e) {
			gestisciEccezione(e);
		}
		return this;
	}
	
	public SimpleDbOp setDate(int position, Date val) {
		if (!checkps())
			return this;
		try {
			if (val==null)
				ps.setNull(position, java.sql.Types.DATE);
			else
				ps.setTimestamp(position, new Timestamp(val.getTime()));
		} catch (Exception e) {
			gestisciEccezione(e);
		}
		return this;
	}

	
	public SimpleDbOp setLong(int position, Long val) {
		if (!checkps())
			return this;
		try {
			if (val==null)
				ps.setNull(position, java.sql.Types.INTEGER);
			else
				ps.setLong(position, val);
		} catch (Exception e) {
			gestisciEccezione(e);
		}
		return this;
	}

	
	public SimpleDbOp setDouble(int position, Double val) {
		if (!checkps())
			return this;
		try {
			if (val==null)
				ps.setNull(position, java.sql.Types.DOUBLE);
			else
				ps.setDouble(position, val);
		} catch (Exception e) {
			gestisciEccezione(e);
		}
		return this;
	}
	
	public SimpleDbOp setString(int position, String val) {
		if (!checkps())
			return this;
		try {
			if (val==null)
				ps.setNull(position, java.sql.Types.VARCHAR);
			else
				ps.setString(position, val);
		} catch (Exception e) {
			gestisciEccezione(e);
		}
		return this;
	}

	public String getString(String key) {
		if (error)
			return null;
		if (rs==null) {
			gestisciEccezione("chiamata a getString() senza il resultset, sql="+sql);
			return null;
		}
		
		try {
			return rs.getString(key);
		} 
		catch (Exception e) {
			gestisciEccezione(e);
			return null;
		}
	}
	
	
	public SimpleDbOp getString(String key, Consumer<String> lambda) {
		String x = getString(key);
		lambda.accept(x);
		return this;
	}


	public double getDouble(String key) {
		return getDouble(key, 0);
	}
	
	public double getDouble(String key, double defValue) {
		if (error)
			return defValue;
		if (rs==null) {
			gestisciEccezione("chiamata a getDouble() senza il resultset, sql="+sql);
			return defValue;
		}
		
		try {
			return rs.getDouble(key);
		} catch (Exception e) {
			gestisciEccezione(e);
			return defValue;
		}
	}


	public double getLong(String key) {
		return getLong(key, 0);
	}
	
	public double getLong(String key, long defValue) {
		if (error)
			return defValue;
		if (rs==null) {
			gestisciEccezione("chiamata a getLong() senza il resultset, sql="+sql);
			return defValue;
		}
		
		try {
			return rs.getLong(key);
		} 
		catch (Exception e) {
			gestisciEccezione(e);
			return defValue;
		}
	}

	public Date getDate(String key) {
		if (error)
			return null;
		if (rs==null) {
			gestisciEccezione("chiamata a getDate senza il resultSet, sql="+sql);
			return null;
		}
		
		try {
			return rs.getTimestamp(key);
		} 
		catch (Exception e) {
			gestisciEccezione(e);
			return null;
		}
	}
	
	//FSTFER3250
	public Clob getClob(String key) {
		if (error)
			return null;
		if (rs==null) {
			gestisciEccezione("chiamata a getClob senza il resultSet, sql="+sql);
			return null;
		}
		
		try {
			return rs.getClob(key);
		} 
		catch (Exception e) {
			gestisciEccezione(e);
			return null;
		}
	}
	
	public SimpleDbOp setClob(int position, Clob val) {
		if (!checkps())
			return this;
		try {
			if (val==null)
				ps.setNull(position, java.sql.Types.CLOB);
			else
				ps.setClob(position, val);
		} catch (Exception e) {
			gestisciEccezione(e);
		}
		return this;
	}

	public SimpleDbOp setClobFromIS(int position, InputStream val) {
		if (!checkps())
			return this;
		try {
			if (val==null)
				ps.setNull(position, java.sql.Types.CLOB);
			else
				ps.setAsciiStream(position, val);
		} catch (Exception e) {
			gestisciEccezione(e);
		}
		return this;
	}
	
	public Blob getBlob(String key) {
		if (error)
			return null;
		if (rs==null) {
			gestisciEccezione("chiamata a getBlob senza il resultSet, sql="+sql);
			return null;
		}
		
		try {
			return rs.getBlob(key);
		} 
		catch (Exception e) {
			gestisciEccezione(e);
			return null;
		}
	}
	
	public String getBlobAsString(String key) {
		if (error)
			return null;
		if (rs==null) {
			gestisciEccezione("chiamata a getBlob senza il resultSet, sql="+sql);
			return null;
		}
		
		try {
			Blob b = rs.getBlob(key);
			if (b==null)
				return null;
			byte bytes[] = IOUtils.toByteArray(b.getBinaryStream());
			return new String(bytes, UTF8.charset);
		} 
		catch (Exception e) {
			gestisciEccezione(e);
			return null;
		}
	}

	public SimpleDbOp setBlob(int position, String val)  {
		byte data[] = val.getBytes(UTF8.charset);
		InputStream s = new ByteArrayInputStream(data);
		return setBlobFromIS(position,  s);
	}

	public SimpleDbOp setBlobFromIS(int position, InputStream val) {
		if (!checkps())
			return this;
		try {
			if (val==null)
				ps.setNull(position, java.sql.Types.BLOB);
			else
				ps.setBytes(position, IOUtils.toByteArray(val));
		} catch (Exception e) {
			gestisciEccezione(e);
		}
		return this;
	}
	
	public SimpleDbOp getInt(String key, Consumer<Integer> lambda) {
		int i=getInt(key);
		lambda.accept(i);
		return this;
	}
	
	public int getInt(String key) {
		if (error)
			return 0;
		if (rs==null) {
			gestisciEccezione("chiamata a getInt senza il resultSet, sql="+sql);
			return 0;
		}
		
		try {
			return rs.getInt(key);
		} 
		catch (Exception e) {
			gestisciEccezione(e);
			return 0;
		}
	}
	
	public SimpleDbOp setInt(int position, int val) {
		if (!checkps())
			return this;
		try {
			ps.setInt(position, val);				
		} catch (Exception e) {
			gestisciEccezione(e);
		}
		return this;
	}
	//FSTFER3250
	
	public SimpleDbOpException getLastError() {
		if (lastException==null)
			return null;
		else if (lastException instanceof SimpleDbOpException)
			return (SimpleDbOpException) lastException;
		else
			return new SimpleDbOpException("SimpleDbOp exception, sql="+sql, lastException);
	}
	
	public SimpleDbOp throwOnError() {
		SimpleDbOpException e = getLastError();
		if (e!=null) {
			close();
			inibisciEccezioni = false;
			throw e;
		}
		else {
			inibisciEccezioni = false;
			return this;			
		}
	}
	
	private boolean checkps() {
		if (error)
			return false;
		else if (ps==null) {
			error = true;
			lastException = new RuntimeException("tentativo di inserire parametri senza aver preparato lo statement, sql="+sql);
			return false;
		}
		else 
			return true;
	}
	//--------------------
	
	
	protected SimpleDbOp logError() {
		if (error) {
			Log.db.log("Errore DBOp, sql="+sql);
			Log.db.log(lastException);
		}
		return this;
	}
	
	
	
	private boolean inibisciEccezioni = true;
	private void gestisciEccezione(String msg) {
		error = true;
		lastException = new SimpleDbOpException(msg);
		if (!inibisciEccezioni) {
			Log.db.warn("SimpleDbOpt exception, chiusura forzata, sql="+sql);
			close();
			throw new SimpleDbOpException(msg);
		}
	}
	
	private void gestisciEccezione(Exception e) {
		error = true;
		lastException = e;
		if (!inibisciEccezioni) {
			Log.db.warn("SimpleDbOpt exception: "+e.getMessage());
			Log.db.warn("chiusura forzata, sql="+sql);
			close();
			throw new SimpleDbOpException(e);
		}
	}

	private void gestisciEccezione(String msg, Exception e) {
		error = true;
		lastException = new SimpleDbOpException(msg, e);
		if (!inibisciEccezioni) {
			Log.db.warn("SimpleDbOpt exception, chiusura forzata, sql="+sql);
			close();
			throw new SimpleDbOpException(msg, e);
		}
	}

	
	
	
	
	protected void prepare() {
		conn = getConnection();
		if (error)
			return;
		try {
			ps = conn.prepareStatement(sql);
		} catch (Exception e) {
			error = true;
			lastException = e;
		}
	}
	
		
	public void clean()  {
		try {
			if (!closed) {
				Log.db.warn("SimpleDBOp Finalize: mancata chiamato di close(), sql="+sql);
				close();
			}
		} 
		catch (Exception e) {
		}
	 }
	
	
}
