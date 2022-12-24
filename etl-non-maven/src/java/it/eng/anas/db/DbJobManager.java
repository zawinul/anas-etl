package it.eng.anas.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.etl.AnasEtlJob;
import it.eng.anas.model.DBJob;

public class DbJobManager<T extends DBJob>  {
	private Connection connection;
	private DBTransactionManager transactionManager;
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
			c.setAutoCommit(false);
			transactionManager = new DBTransactionManager(c);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
    public DbJobManager(Connection connection) {
		super();
		this.connection = connection;
		try {
			connection.setAutoCommit(false);
			transactionManager = new DBTransactionManager(connection);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


//	public  DBJob insertNew(String queue, int priority,  
//			String operation, String key1, String key2, String key3, int parentJob, String body)  throws Exception {
//		String time = Utils.date2String(new Date());
//		DBJob job = new DBJob(
//			-1, //id
//			null, // lock
//			priority,
//			0, // n retry
//			queue, 
//			operation,
//			key1, key2, key3, 
//			time, time, 
//			parentJob,
//			0, // duration
//			body
//		);		
//		return insertNew(job);
//	}

	public synchronized T insertNew(T job)  throws Exception {
		return transactionManager.execute(new Callable<T>() {
			public T call() throws Exception {
				return _insertNew(job);
			}
		});
	}


	public /*synchronized */ T extract()  throws Exception {
		return transactionManager.execute(new Callable<T>() {
			public synchronized T call() throws Exception {
				return _extract2();
			}
		});
	}
	

	public synchronized T ack(T job, String out)  throws Exception {

		return transactionManager.execute(new Callable<T>() {
			public synchronized T call() throws Exception {
				return _ack(job, out);
			}
		});
	}

	public synchronized T nack(T job, String out)  throws Exception {
		return transactionManager.execute(new Callable<T>() {
			public synchronized T call() throws Exception {
				return _nack(job, out);
			}
		});
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
		
		insert(job, "job");
		
		return job;
	}
	
	private void insert(T job, String table) throws Exception {
		String insertSql = "insert into "
				+ table
				+ " (jobid,priority,locktag,nretry,queue,operation,key1,key2,key3,creation,last_change,parent_job,duration,body,output) "
				+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		
		
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
			.setString(14, mapper.writeValueAsString(job))
			.setString(15, limit(job.output, 500))
			.execute()
			.close()
			.throwOnError();		
	}
	
	private static final String getLockSql = 
		" UPDATE job SET locktag=?, last_change=? WHERE jobid= ( "+
		"   (SELECT jobid                                        "+
		"     FROM (select * from job as job2 ) as j2            "+
		"     WHERE locktag is null AND ( extractCondition )     "+
		"     ORDER BY priority desc,nretry                      "+
		"     LIMIT 1                                            "+
		"   )                                                    "+
		" )                                                      ";
	// la doppia select è necessaria, vedi https://stackoverflow.com/questions/44970574/table-is-specified-twice-both-as-a-target-for-update-and-as-a-separate-source
	
	private T _extract2()  throws Exception {
		String extractCondition = Utils.getConfig().extractCondition;
		if (extractCondition==null)
			extractCondition="1=1";
		String query = getLockSql.replace("extractCondition", extractCondition);
		String lcktag = Utils.rndString(6);
		String now = Utils.date2String(new Date());
		SimpleDbOp op1 = new SimpleDbOp(connection)
				.query(query)
				.setString(1, lcktag)
				.setString(2, now)
				.executeUpdate()
				.close()
				.throwOnError();
		
		try {
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int n = op1.getNumOfExecutedUpdate();
		if (n<=0) {
			//Log.db.log("coda vuota");
			return null;
		}
		
		String sqlget = "select body, priority from job where locktag=?";
		SimpleDbOp op2 = new SimpleDbOp(connection)
				.query(sqlget)
				.setString(1, lcktag)
				.executeQuery()
				.throwOnError();
		if (!op2.next())
			throw new RuntimeException("non dovrebbe mai accadere: lcktag="+lcktag);
		String jsonBody = op2.getString("body");
		
		// la priority potrebbe essere stata cambiata da sql
		// aggiorniamo il body
		int priority = op2.getInt("priority");
		
		op2.close();
		
		T ret = mapper.readValue(jsonBody, tclass);
		ret.priority = priority;
		return ret;
	}

	private T _ack(T job, String out)  throws Exception  {
		job.output = out;
		updateTiming(job);
		//insert(job, "job_done");
		String sql = "update job set locktag=?, output=?, last_change=?, duration=?  where jobid=?";
		new SimpleDbOp(connection)
			.query(sql)
			.setString(1, "ack")
			.setString(2, job.output)
			.setString(3, job.last_change)
			.setInt(4, job.duration)
			.setString(5, job.id)
			.executeUpdate().close().throwOnError();
		return job;
	}
	private T _nack(T job, String out)  throws Exception {
		job.output = out;
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
				.setString(4, limit(job.output, 500))
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
				.setString(2, limit(job.output, 500))
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
