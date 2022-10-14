package it.eng.anas.db;

import java.sql.Connection;
import java.util.Date;
import java.util.function.Consumer;

import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.model.DBJob;

public class DbJobManager {
	private Connection connection;
	
	public DbJobManager() {}
	public DbJobManager(Connection connection) {
		super();
		this.connection = connection;
	}

	public DBJob createNew(int id, String objectid, Integer priority, DBJob.Status status, String queue, String creation,
			String last_change, String command, String body) {
		DBJob ret = new DBJob();
		ret.id = id;
		ret.objectid = objectid;
		ret.priority = priority;
		ret.status = status;
		ret.queue = queue;
		ret.creation = creation;
		ret.last_change = last_change;
		ret.command = command;
		ret.body = body;
		return ret;
	}

	public DBJob insertNew(String queue, String objectid, int priority,  String command, String body) {
		String time = Utils.date2String(new Date());
		DBJob job = createNew(-1, objectid, priority,DBJob.Status.ready,queue,time, time, command, body);
		
		return insertNew(job);
	}

	public DBJob insertNew(DBJob job) {
		SimpleDbOp op =new SimpleDbOp(connection)
			.query("select id from jobid_sequence")
			.executeQuery();
		op.next();
		int id = op.getInt("id");
		op.close()
			.throwOnError();

		job.id = id;
		job.creation = job.last_change = Utils.date2String(new Date());
		String insertSql = "insert into job(jobid,objectid,priority,status,queue,creation,last_change,command,body) values(?,?,?,?,?,?,?,?,?)";
		SimpleDbOp op2 = new SimpleDbOp(connection)
			.query(insertSql)
			.setInt(1, job.id)
			.setString(2, job.objectid)
			.setInt(3, job.priority)
			.setString(4, job.status.toString())
			.setString(5, job.queue)
			.setString(6, job.creation)
			.setString(7, job.last_change)
			.setString(8, job.command)
			.setBlob(9, job.body)
			.execute()
			.close()
			.throwOnError();
		
		new SimpleDbOp(connection)
			.query("update jobid_sequence set id=?")
			.setInt(1, id+1)
			.execute()
			.close()
			.throwOnError();

		return job;
	}
	
	public DBJob extract(String queue) throws Exception {
		Connection con = connection;
		if (con==null)
			con = DBConnectionFactory.defaultFactory.getConnection();
		boolean savedAutoCommit = con.getAutoCommit();
		con.setAutoCommit(false);
		try {
			String sql1 = "select * from job where queue=? and status=? order by priority desc limit 1";
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
			con.commit();
			return ret;
		} catch (Exception e) {
			con.rollback();
			e.printStackTrace();
			return null;
		}
		finally {
			con.setAutoCommit(savedAutoCommit);
		}
	}
	
	public DBJob ack(DBJob job) throws Exception {
		job.status = DBJob.Status.done;
		job.last_change = Utils.date2String(new Date());
		String sql = "update job set status=?, last_change=? where jobid=?";
		new SimpleDbOp(connection)
			.query(sql)
			.setString(1, job.status.toString())
			.setString(2, job.last_change)
			.setInt(3, job.id)
			.execute().close().throwOnError();
		return job;
	}

	
	public DBJob nack(DBJob job) {
		job.status = DBJob.Status.error;
		job.last_change = Utils.date2String(new Date());
		String sql = "update job set status=?, last_change=? where jobid=?";
		new SimpleDbOp(connection)
			.query(sql)
			.setString(1, job.status.toString())
			.setString(2, job.last_change)
			.setInt(3, job.id)
			.execute().close().throwOnError();
		return job;
	}

	public DBJob fromDB(SimpleDbOp op) {
		if (!op.next())
			return null;
		final DBJob ret = new DBJob();
		ret.id = op.getInt("jobid");
		ret.objectid = op.getString("objectid");
		ret.priority = op.getInt("priority");
		String status=op.getString("status");
		//Log.db.log("status="+status);
		ret.status = DBJob.Status.valueOf(status);
		ret.queue = op.getString("queue");
		ret.creation = op.getString("creation");
		ret.last_change = op.getString("last_change");
		ret.command = op.getString("command");
		ret.body = op.getBlobAsString("body");

		return ret;
	} 
	
	public static int test=1;
	public static void main(String args[]) throws Exception {
		if (test==1) {
			DbJobManager m = new DbJobManager();
			DBJob job = new DBJob();
			for(int i=0; i<10;i++)
				m.insertNew("esempio1", "{"+Utils.rndString(4)+"}", 7, Utils.rndString(6), Utils.rndString(30));
			Log.db.log("ok");
		}
		else if (test==2) {
			DbJobManager m = new DbJobManager();
			SimpleDbOp op = new SimpleDbOp()
				.query("select * from job")
				.executeQuery();
			while(true) {
				DBJob job = m.fromDB(op);
				if (job==null)
					break;
				System.out.println(job);
			}
			op.throwOnError();
		}
	}
}
