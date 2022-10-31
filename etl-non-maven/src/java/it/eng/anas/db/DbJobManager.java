package it.eng.anas.db;

import java.sql.Connection;
import java.util.Date;
import java.util.concurrent.Callable;

import it.eng.anas.Utils;
import it.eng.anas.model.DBJob;

public class DbJobManager  {
	private Connection connection;
	private DBTransactionManager transactionManager;
	public DbJobManager() {
		try {
			Connection c = DBConnectionFactory.defaultFactory.getConnection("dbJobManagerConstructor");
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
			String operation, String par1, String par2, String par3, int parentJob, String extra) {
		String time = Utils.date2String(new Date());
		DBJob job = new DBJob(
			-1, //id
			DBJob.Status.ready,
			priority,
			0, // n retry
			queue, 
			operation,
			par1, par2, par3, 
			time, time, 
			parentJob,
			0, // duration
			extra
		);		
		return insertNew(job);
	}

	public synchronized DBJob insertNew(DBJob job) {
		return transactionManager.execute(new Callable<DBJob>() {
			public DBJob call() throws Exception {
				return _insertNew(job);
			}
		});
	}


	public synchronized DBJob extract(String queue) {
		return transactionManager.execute(new Callable<DBJob>() {
			public DBJob call() throws Exception {
				return _extract(queue);
			}
		});
	}
	

	public synchronized DBJob ack(DBJob job, String out) {
		return transactionManager.execute(new Callable<DBJob>() {
			public DBJob call() throws Exception {
				return _ack(job, out);
			}
		});
	}

	public synchronized DBJob nack(DBJob job, String out) {
		return transactionManager.execute(new Callable<DBJob>() {
			public DBJob call() throws Exception {
				return _nack(job, out);
			}
		});
	}
	
	private DBJob _insertNew(DBJob job) {
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
	
	private void insert(DBJob job, String table) {
		String insertSql = "insert into "
				+ table
				+ " (jobid,priority,status,nretry,queue,operation,par1,par2,par3,creation,last_change,parent_job,duration,extra) "
				+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		
		new SimpleDbOp(connection)
			.query(insertSql)
			.setInt(1, job.id)
			.setInt(2, job.priority)
			.setString(3, job.status.toString())
			.setInt(4, job.nretry)
			.setString(5, job.queue)
			.setString(6, job.operation)
			.setString(7, job.par1)
			.setString(8, job.par2)
			.setString(9, job.par3)
			.setString(10, job.creation)
			.setString(11, job.last_change)
			.setInt(12, job.parent_job)
			.setInt(13, job.duration)
			.setBlob(14, job.extra)
			.execute()
			.close()
			.throwOnError();		
	}
	
	private DBJob _extract(String queue)  {
		String sql1 = "select * from job where queue=? and status=? order by priority desc, nretry, operation, par1, par2, par3 limit 1";
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
	
	private DBJob _ack(DBJob job, String out)  {
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

	
	private DBJob _nack(DBJob job, String out) {
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
		ret.status = DBJob.Status.valueOf(status);
		ret.queue = op.getString("queue");
		ret.operation = op.getString("operation");
		ret.par1 = op.getString("par1");
		ret.par2 = op.getString("par2");
		ret.par3 = op.getString("par3");
		ret.creation = op.getString("creation");
		ret.last_change = op.getString("last_change");
		
		ret.parent_job = op.isNull("parent_job") ? null : op.getInt("parent_job");
		ret.extra = op.getBlobAsString("extra");
		
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
