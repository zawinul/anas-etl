package it.eng.anas.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.Callable;

import it.eng.anas.Utils;
import it.eng.anas.model.DBJob;

public class DbJobManager  {
	private Connection connection;
	private DBTransactionManager transactionManager;
	public String tag;
	
	public DbJobManager() {
		this(Utils.rndString(5));
	}
	
	public DbJobManager(String tag) {
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


	public  DBJob insertNew(String queue, int priority,  
			String operation, String key1, String key2, String key3, int parentJob, String body)  throws Exception {
		String time = Utils.date2String(new Date());
		DBJob job = new DBJob(
			-1, //id
			DBJob.Status.ready,
			priority,
			0, // n retry
			queue, 
			operation,
			key1, key2, key3, 
			time, time, 
			parentJob,
			0, // duration
			body
		);		
		return insertNew(job);
	}

	public  DBJob insertNew(DBJob job)  throws Exception {
		return transactionManager.execute(new Callable<DBJob>() {
			public DBJob call() throws Exception {
				return _insertNew(job);
			}
		});
	}


	public DBJob extract(String queue)  throws Exception {
		return transactionManager.execute(new Callable<DBJob>() {
			public synchronized DBJob call() throws Exception {
				return _extract2(queue);
			}
		});
	}
	

	public DBJob ack(DBJob job, String out)  throws Exception {
		return transactionManager.execute(new Callable<DBJob>() {
			public synchronized DBJob call() throws Exception {
				return _ack(job, out);
			}
		});
	}

	public DBJob nack(DBJob job, String out)  throws Exception {
		return transactionManager.execute(new Callable<DBJob>() {
			public synchronized DBJob call() throws Exception {
				return _nack(job, out);
			}
		});
	}
	
	private DBJob _insertNew(DBJob job)  throws Exception {
		SimpleDbOp op = new SimpleDbOp(connection)
			.query("select id from jobid_sequence")
			.executeQuery();
		op.next();
		int id = op.getInt("id");
		op.close()
			.throwOnError();

		job.id = id;
		job.creation = job.last_change = Utils.date2String(new Date());
		
		insert(job, "job");
		
		new SimpleDbOp(connection)
			.query("update jobid_sequence set id=?")
			.setInt(1, id+1)
			.execute()
			.close()
			.throwOnError();
		return job;
	}
	
	private void insert(DBJob job, String table) throws Exception {
		String insertSql = "insert into "
				+ table
				+ " (jobid,priority,status,nretry,queue,operation,key1,key2,key3,creation,last_change,parent_job,duration,body,output) "
				+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		
		String out = job.output;
		if (out!=null && out.length()>500)
			out = out.substring(0,500);
		
		new SimpleDbOp(connection)
			.query(insertSql)
			.setInt(1, job.id)
			.setInt(2, job.priority)
			.setString(3, job.status.toString())
			.setInt(4, job.nretry)
			.setString(5, job.queue)
			.setString(6, job.operation)
			.setString(7, job.key1)
			.setString(8, job.key2)
			.setString(9, job.key3)
			.setString(10, job.creation)
			.setString(11, job.last_change)
			.setInt(12, job.parent_job)
			.setInt(13, job.duration)
			.setString(14, job.body)
			.setString(15, out)
			.execute()
			.close()
			.throwOnError();		
	}
	
	@SuppressWarnings("unused")
	private DBJob _extract(String queue)  throws Exception {
		String sql1 = "select * from job where queue=? and status=? order by priority desc, nretry, operation, key1, key2, key3 limit 1";
		SimpleDbOp op1 = new SimpleDbOp(connection)
				.query(sql1)
				.setString(1, queue)
				.setString(2, DBJob.Status.ready.toString()) 
				.executeQuery()
				.throwOnError();
		DBJob ret = fromDB(op1);
		op1.close();

		if (ret!=null) {
			ret.status = DBJob.Status.process;
			ret.last_change = Utils.date2String(new Date());
			String sql2 = "update job set status=?, last_change=? where jobid=?";
			new SimpleDbOp(connection)
				.query(sql2)
				.setString(1, ret.status.toString())
				.setString(2, ret.last_change)
				.setInt(3, ret.id)
				.execute().close().throwOnError();			
		}
		return ret;
	}

	
	private DBJob _extract2(String queue)  throws Exception {
		String lcktag = "LCK-"+Utils.rndString(6);
		String getLockSql = 
				"UPDATE job SET status=? WHERE jobid= ("
				+ "	(SELECT jobid "
				+ "        FROM (select * from job as job2 ) as j2"
				+ "        WHERE status='ready' "
				+ "        ORDER BY priority desc,nretry,key1,key2,key3,jobid "
				+ "        LIMIT 1"
				+ "    )  "
				+ ")";

		SimpleDbOp op1 = new SimpleDbOp(connection)
				.query(getLockSql)
				.setString(1, lcktag)
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
		
		String sqlget = "select * from job where status=?";
		SimpleDbOp op2 = new SimpleDbOp(connection)
				.query(sqlget)
				.setString(1, lcktag)
				.executeQuery()
				.throwOnError();
		
		DBJob ret = fromDB(op2);
		if (ret==null) 
			throw new RuntimeException("non dovrebbe mai accadere: lcktag="+lcktag);
		
		ret.status = DBJob.Status.process;
		ret.last_change = Utils.date2String(new Date());
		op2.close();
		
		String sqlSetStatus = "update job set status=?, last_change=? where jobid=?";
		new SimpleDbOp(connection)
			.query(sqlSetStatus)
			.setString(1, ret.status.toString())
			.setString(2, ret.last_change)
			.setInt(3, ret.id)
			.execute()
			.throwOnError();			

		return ret;
	}

	private DBJob _ack(DBJob job, String out)  throws Exception  {
		job.output = out;
		job.status = DBJob.Status.done;
		updateTiming(job);
		insert(job, "job_done");
		
		String sql = "delete from job where jobid=?";
		new SimpleDbOp(connection)
			.query(sql)
			.setInt(1, job.id)
			.execute().close().throwOnError();
		return job;
	}

	
	private DBJob _nack(DBJob job, String out)  throws Exception {
		job.output = out;
		updateTiming(job);
		job.nretry++;
		if (job.nretry<Utils.getConfig().nMaxRetry) {
			// requeue for another retry
			job.status = DBJob.Status.ready;
			insert(job, "job");
		}
		else {
			// move to job_error
			job.status = DBJob.Status.ready;
			insert(job, "job_error");
			String sql = "delete from job where jobid=?";
			new SimpleDbOp(connection)
				.query(sql)
				.setInt(1, job.id)
				.execute().close().throwOnError();
		}
		return job;
	}
	
	public DBJob fromDB(SimpleDbOp op) {
		if (!op.next())
			return null;
		final DBJob ret = new DBJob();
		ret.id = op.getInt("jobid");
		ret.priority = op.getInt("priority");
		String status=op.getString("status");
		//Log.db.log("status="+status);
		ret.nretry = op.getInt("nretry");
		try {
			ret.status = DBJob.Status.valueOf(status);
		}catch(Exception e) {}
		ret.queue = op.getString("queue");
		ret.operation = op.getString("operation");
		ret.key1 = op.getString("key1");
		ret.key2 = op.getString("key2");
		ret.key3 = op.getString("key3");
		ret.creation = op.getString("creation");
		ret.last_change = op.getString("last_change");
		
		ret.parent_job = op.isNull("parent_job") ? null : op.getInt("parent_job");
		ret.body = op.getString("body");
		ret.output = op.getString("output");
		
		return ret;
	} 
	

	private void updateTiming(DBJob job) {
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

}
