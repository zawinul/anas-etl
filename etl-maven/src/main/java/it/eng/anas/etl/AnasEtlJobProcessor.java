package it.eng.anas.etl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Event;
import it.eng.anas.FileHelper;
import it.eng.anas.Utils;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.model.DBJob;

public class AnasEtlJobProcessor  {

	private ObjectMapper mapper;
	private FileHelper fileHelper = new FileHelper();
	private DbJobManager manager;

	
	public AnasEtlJobProcessor(DbJobManager manager) {
		this.manager = manager;
	}
	
	public void process(DBJob job) {
		if (job.operation.equals("getDbsId"))
			getDbsId(job);

		if (job.operation.equals("getDbsMd"))
			getDbsMd(job);

		if (job.operation.equals("getDbsTree"))
			getDbsTree(job);

		if (job.operation.equals("getFolderMd"))
			getFolderMd(job);

		if (job.operation.equals("getDocMD"))
			getDocMD(job);

		if (job.operation.equals("getDocContent"))
			getDocContent(job);

	}
	
	private void getDbsId(DBJob job) {
		String className = job.par1;
		for(int i=0; i<20; i++) {
			String dbsId = UUID.randomUUID().toString();
			
			manager.insertNew(
					"anas-etl", // queue
					990, // priority
					"getDbsMd", // operation
					className, // par1
					dbsId, // par2
					null, // par3
					job.id, //parentJob,
					null // extra
			);
			
			manager.insertNew(
					"anas-etl", // queue
					980, // priority
					"getDbsTree", // operation
					className, // par1
					dbsId, // par2
					null, // par3
					job.id, //parentJob,
					null // extra
			);
		}
	}
	
	
	private void getDbsMd(DBJob job) {
		String className = job.par1;
		String dbsId = job.par2;
		
		fileHelper.saveJsonObject(className+"/"+dbsId+"/dbs-metadata.json", job);
	}
	
	private void getDbsTree(DBJob job) {
		String className = job.par1, dbsId=job.par2;
		for(int i=0; i<20; i++) {
			String folderId = UUID.randomUUID().toString();
			
			manager.insertNew(
					"anas-etl", // queue
					800, // priority
					"getFolderMd", // operation
					className, // par1
					dbsId, // par2
					folderId, // par3
					job.id, //parentJob,
					null // extra
			);
		}
		
	}
	
	private void getFolderMd(DBJob job) {
		String className = job.par1, dbsId=job.par2, folderId=job.par3;
		String f1 = folderId.substring(0,18);
		String f2 = folderId.substring(19);
		String path = className+"/"+dbsId+"/"+f1+"/"+f2;
		fileHelper.saveJsonObject(path+"/folder-metadata.json", job);
		for(int i=0;i<20; i++) {
			String docId = UUID.randomUUID().toString();
			manager.insertNew(
					"anas-etl", // queue
					800, // priority
					"getDocMD", // operation
					className, // par1
					dbsId, // par2
					docId, // par3
					job.id, //parentJob,
					path // extra
			);
			manager.insertNew(
					"anas-etl", // queue
					760, // priority
					"getDocContent", // operation
					className, // par1
					dbsId, // par2
					docId, // par3
					job.id, //parentJob,
					path // extra
			);
		}		
	}
	
	private void getDocMD(DBJob job) {
		String className = job.par1, dbsId=job.par2, docId=job.par3, path=job.extra;
		fileHelper.saveJsonObject(path+"/"+docId+".metadata.json", job);
	}
	
	private void getDocContent(DBJob job) {
		Utils.sleep(10000);
		String className = job.par1, dbsId=job.par2, docId=job.par3, path=job.extra;
		fileHelper.saveJsonObject(path+"/"+docId+".bin", job);
	}

	public static void startSimulation(String docClass) {
		try {
			Connection connection = DBConnectionFactory.defaultFactory.getConnection();
			DbJobManager manager = new DbJobManager(connection);

			manager.insertNew(
					"anas-etl", // queue
					990, // priority
					"getDbsId", // operation
					docClass, // par1
					null, // par2
					null, // par3
					-1, //parentJob,
					null // extra
			);
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
