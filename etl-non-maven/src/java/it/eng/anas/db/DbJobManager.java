package it.eng.anas.db;

import java.sql.Connection;
import java.util.Date;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.model.DBJob;

public class DbJobManager<T extends DBJob>  {
	private Connection connection;
	public String tag;
	private ObjectMapper mapper = Utils.getMapperOneLine();
	private Class<T> tclass; 
	
	public DbJobManager() {
		this(Utils.rndString(5), null);
	}
	
	public DbJobManager(String tag, Class<T> tclass) {
		this.tclass = tclass;
		this.tag = tag;
		try {
			Connection c = DBConnectionFactory.defaultFactory.getConnection("dbJobManager "+tag);
			this.connection = c;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
    public DbJobManager(Connection connection) {
		super();
		this.connection = connection;
	}


	public /* synchronized */ T  insertNew(T job)  throws Exception {
		return _insertNew(job);
	}


	public final static Object extractLock ="";
	public  T extract()  throws Exception {
		synchronized(extractLock) {
			return _extract3();
		}
	}
	

	public synchronized T ack(T job, String out)  throws Exception {
		return _ack(job, out);
	}

	public synchronized T nack(T job, String out)  throws Exception {
		return _nack(job, out);
	}
	


	private T _insertNew(T job)  throws Exception {
		job.id = Utils.rndString(10);
		job.creation = job.last_change = Utils.date2String(new Date());
		
		String time = Utils.date2String(new Date());
		job.nretry = 0;
		job.duration = 0;
		job.creation = time;
		job.last_change = time;
		job.locktag = null;
		
		insert(job, "job", null);
		
		return job;
	}
	
	private void insert(T job, String table, String output) throws Exception {
		String insertSql = "insert into "
				+ table
				+ " (jobid,priority,locktag,nretry,queue,operation,key1,key2,key3,creation,last_change,parent_job,duration,body,output) "
				+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		
		ObjectNode  clone = mapper.valueToTree(job);
		clone.remove("body");
		clone.remove("output");
		
		new SimpleDbOp(connection)
			.query(insertSql)
			.setString(1, job.id)
			.setInt(2, job.priority)
			.setString(3, job.locktag)
			.setInt(4, job.nretry)
			.setString(5, job.queue)
			.setString(6, job.operation)
			.setString(7, job.key1)
			.setString(8, job.key2)
			.setString(9, job.key3)
			.setString(10, job.creation)
			.setString(11, job.last_change)
			.setString(12, job.parent_job)
			.setInt(13, job.duration)
			.setString(14, mapper.writeValueAsString(clone))
			.setString(15, limit(output, 450))
			.execute()
			.close()
			.throwOnError();		
	}

	
//	private static final String getLockSql = 
//		" UPDATE job SET locktag=?, last_change=? WHERE jobid= ( "+
//		"   (SELECT jobid                                        "+
//		"     FROM (select * from job as job2 ) as j2            "+
//		"     WHERE locktag is null AND ( extractCondition )     "+
//		"     ORDER BY priority desc,nretry                      "+
//		"     LIMIT 1                                            "+
//		"   )                                                    "+
//		" )                                                      ";
//	// la doppia select è necessaria, vedi https://stackoverflow.com/questions/44970574/table-is-specified-twice-both-as-a-target-for-update-and-as-a-separate-source
//	
//	private T _extract2()  throws Exception {
//		String extractCondition = Utils.getConfig().extractCondition;
//		if (extractCondition==null)
//			extractCondition="1=1";
//		String query = getLockSql.replace("extractCondition", extractCondition);
//		String lcktag = Utils.rndString(6);
//		String now = Utils.date2String(new Date());
//		SimpleDbOp op1 = new SimpleDbOp(connection)
//				.query(query)
//				.setString(1, lcktag)
//				.setString(2, now)
//				.executeUpdate()
//				.close()
//				.throwOnError();
//		
//		int n = op1.getNumOfExecutedUpdate();
//		if (n<=0) {
//			//Log.db.log("coda vuota");
//			return null;
//		}
//		
//		String sqlget = "select body, priority from job where locktag=?";
//		SimpleDbOp op2 = new SimpleDbOp(connection)
//				.query(sqlget)
//				.setString(1, lcktag)
//				.executeQuery()
//				.throwOnError();
//		if (!op2.next())
//			throw new RuntimeException("non dovrebbe mai accadere: lcktag="+lcktag);
//		String jsonBody = op2.getString("body");
//		
//		// la priority potrebbe essere stata cambiata da sql
//		// aggiorniamo il body
//		int priority = op2.getInt("priority");
//		
//		op2.close();
//		
//		T ret = mapper.readValue(jsonBody, tclass);
//		ret.priority = priority;
//		return ret;
//	}

	private T _extract3()  throws Exception {
		String extractCondition = Utils.getConfig().extractCondition;
		if (extractCondition==null)
			extractCondition="1=1";
		String sqlget = "select jobid,  body, priority from job "
				+ "WHERE locktag is null AND ("+extractCondition+") "
				+ "ORDER BY priority desc,nretry LIMIT 1";

		SimpleDbOp op2 = new SimpleDbOp(connection)
				.query(sqlget)
				.executeQuery()
				.throwOnError();
		if (!op2.next()) {
			op2.close();
			return null;
		}
		String id = op2.getString("jobid");
		String jsonBody = op2.getString("body");		
		int priority = op2.getInt("priority");
		op2.close();
		
		Log.log(jsonBody);
		T ret = null;
		try {
			ret = mapper.readValue(jsonBody, tclass);
		}catch(Exception e) {
			e.printStackTrace();
			Log.log(e);
			new SimpleDbOp(connection)
				.query("update job set locktag='nack' where jobid=?")
				.setString(1,  id)
				.executeUpdate()
				.throwOnError();
			return null;
		}
		// la priority potrebbe essere stata cambiata da sql
		// aggiorniamo il body
		ret.priority = priority;

		String lcktag = Utils.rndString(6);
		String now = Utils.date2String(new Date());
		
		String upd="update job set locktag=?,last_change=? where jobid=?";
		new SimpleDbOp(connection)
			.query(upd)
			.setString(1, lcktag)
			.setString(2, now)
			.setString(3, id)
			.executeUpdate()
			.close()
			.throwOnError();
		
		return ret;
	}


	private T _ack(T job, String out)  throws Exception  {
		if (!Utils.getConfig().saveJobDone) {
			String sql = "delete from job where jobid=?";
			new SimpleDbOp(connection)
				.query(sql)
				.setString(1, job.id)
				.execute().close().throwOnError();
			return job;
		}
		updateTiming(job);
		//insert(job, "job_done");
		String sql = "update job set locktag=?, output=?, last_change=?, duration=?  where jobid=?";
		new SimpleDbOp(connection)
			.query(sql)
			.setString(1, "ack")
			.setString(2, limit(out, 500))
			.setString(3, job.last_change)
			.setInt(4, job.duration)
			.setString(5, job.id)
			.executeUpdate().close().throwOnError();
		return job;
	}
	private T _nack(T job, String out)  throws Exception {
		updateTiming(job);
		job.nretry++;
		job.last_change = Utils.date2String(new Date());
		if (job.nretry<Utils.getConfig().nMaxRetry) {
			// reinoltro in coda togliendo il lock e incrementando il n.retry
			new SimpleDbOp(connection)
				.query("UPDATE job SET locktag=null, last_change=?, nretry=?, body=?, output=? WHERE jobid=?")
				.setString(1, job.last_change)
				.setInt(2, job.nretry)
				.setString(3, limit(mapper.writeValueAsString(job), 2000))
				.setString(4, limit(out, 450))
				.setString(5, job.id)
				.executeUpdate()
				.close()
				.throwOnError();
		}
		else {
			
			String sql = "update job set locktag=?, output=?, last_change=?, duration=?  where jobid=?";
			new SimpleDbOp(connection)
				.query(sql)
				.setString(1, "nack")
				.setString(2, limit(out, 450))
				.setString(3, job.last_change)
				.setInt(4, job.duration)
				.setString(5, job.id)
				.executeUpdate().close().throwOnError();
			return job;
		}
		return job;
	}
	

	private void updateTiming(T job) {
		Date t0 = Utils.string2Date(job.last_change);
		Date t1 = new Date();
		int delta = (int)(t1.getTime()-t0.getTime());
		job.last_change = Utils.date2String(t1);
		job.duration += delta;		
	}

	public void close() {
		if (connection!=null)
			DBConnectionFactory.close(connection);
		connection = null;
	}
	
	private String limit(String x, int maxlen) {
		if (x==null)
			return null;
		else if (x.length()>maxlen)
			return x.substring(0,maxlen);
		else
			return x;
	}

//	private int getNextId() throws Exception {
//		new SimpleDbOp(connection)
//			.query("LOCK TABLES jobid_sequence WRITE")
//			.execute()
//			.close()
//			.throwOnError();
//		
//		SimpleDbOp sel = new SimpleDbOp(connection)
//			.query("select id from jobid_sequence")
//			.executeQuery();
//		
//		sel.next();
//		int id = sel.getInt("id");
//		sel.close();
//		
//		new SimpleDbOp(connection)
//			.query("update jobid_sequence set id=?")
//			.setInt(1, id+1)
//			.executeUpdate()
//			.close();
//		
//		new SimpleDbOp(connection)
//			.query("UNLOCK TABLES ")
//			.execute()
//			.close()
//			.throwOnError();
//			
//		
//		return id;
//	}
	
}
