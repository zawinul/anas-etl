package it.eng.anas.etl;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.filenet.api.collection.ClassDescriptionSet;
import com.filenet.api.collection.PageIterator;
import com.filenet.api.collection.PropertyDescriptionList;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.meta.ClassDescription;
import com.filenet.api.meta.PropertyDescription;

import it.eng.anas.FileHelper;
import it.eng.anas.FilenetHelper;
import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.model.DBJob;

public class AnasEtlJobProcessor  {

	private AnasEtlWorker caller;
	private FileHelper fileHelper = new FileHelper();
	public AnasEtlJobProcessor(AnasEtlWorker caller) {
		this.caller = caller;
	}
	
	public void process(DBJob job) throws Exception {
		if (job.operation.equals("getDbs"))
			getDbs(job);

		else if (job.operation.equals("getDbsMd"))
			getDbsMd(job);

		else if (job.operation.equals("getDbsTree"))
			getDbsTree(job);

		else if (job.operation.equals("getFolderMd"))
			getFolderMd(job);

		else if (job.operation.equals("getDocId"))
			getDocId(job);

		else if (job.operation.equals("getDocMD"))
			getDocMD(job);

		else if (job.operation.equals("getDocContent"))
			getDocContent(job);


		else if (job.operation.equals("getClassStruct"))
			getClassStruct(job);
		
		else {
			caller.getJobManager().nack(job);
			throw new Exception("operation non riconosciuta: "+job.operation+", job="+Utils.getMapperOneLine().writeValueAsString(job));
		}
		
	}
	
	private void getDbs(DBJob job) throws Exception {
		String parentPath = job.par1;
		FilenetHelper fnet = caller.getFilenetHelper();
		DbJobManager jobManager = caller.getJobManager();

		List<String> paths = fnet.getDBSList(parentPath);
		for(String path: paths) {
			jobManager.insertNew(
					"anas-etl", // queue
					990, // priority
					"getDbsMd", // operation
					parentPath, // par1
					path, // par2
					null, // par3
					job.id, //parentJob,
					null // extra
			);
			
			jobManager.insertNew(
					"anas-etl", // queue
					980, // priority
					"getDbsTree", // operation
					parentPath, // par1
					path, // par2
					null, // par3
					job.id, //parentJob,
					null // extra
			);
		}
	}
	
	
	private void getDbsMd(DBJob job)  throws Exception {
		String parenbtPath = job.par1;
		String path = job.par2;
		
		ObjectNode node = caller.getFilenetHelper().getFolderMetadata(path);
		fileHelper.saveJsonObject(path+"/dbs-metadata.json", node);
	}
	
	private void getDbsTree(DBJob job)  throws Exception {
		String parentPath = job.par1, path=job.par2;
		DbJobManager jobManager = caller.getJobManager();
		FilenetHelper fnet = caller.getFilenetHelper();
		List<String> paths = fnet.getRecursiveFolders(path);
		for(String sub: paths) {
			jobManager.insertNew(
					"anas-etl", // queue
					800, // priority
					"getFolderMd", // operation
					parentPath, // par1
					sub, // par2
					null, // par3
					job.id, //parentJob,
					null // extra
			);
			
			jobManager.insertNew(
					"anas-etl", // queue
					790, // priority
					"getDocId", // operation
					parentPath, // par1
					sub, // par2
					null, // par3
					job.id, //parentJob,
					null // extra
			);

		}
	}
	
	private void getFolderMd(DBJob job)  throws Exception {
		String parentPath = job.par1, path=job.par2;
		FilenetHelper fnet = caller.getFilenetHelper();
		ObjectNode metadata = fnet.getFolderMetadata(path);
		fileHelper.saveJsonObject(path+"/folder-metadata.json", metadata);
	}
	
	@SuppressWarnings("unused")
	private void getDocId(DBJob job)  throws Exception {
		String parentPath = job.par1, path=job.par2;
		FilenetHelper fnet = caller.getFilenetHelper();
		DbJobManager jobManager = caller.getJobManager();
		List<String> docIds = fnet.getDocumentsId(path);
		for(String docId: docIds) {
			jobManager.insertNew(
					"anas-etl", // queue
					780, // priority
					"getDocMD", // operation
					parentPath, // par1
					path, // par2
					docId, // par3
					job.id, //parentJob,
					null // extra
			);			
		}
	}
	
	
	@SuppressWarnings("unused")
	private void getDocMD(DBJob job)  throws Exception {
		String parentPath = job.par1, path=job.par2, docId=job.par3;
		FilenetHelper fnet = caller.getFilenetHelper();
		ObjectNode metadata = fnet.getDocumentMetadata(docId);
		fileHelper.saveJsonObject(path+"/"+docId, metadata);
	}

	@SuppressWarnings("unused")
	private void getDocContent(DBJob job)  throws Exception {
		Utils.randomSleep(1000,3000);
		String className = job.par1, dbsId=job.par2, docId=job.par3, path=job.extra;
		fileHelper.saveJsonObject(path+"/"+docId+".bin", job);
	}

	
	private void getClassStruct(DBJob job) throws Exception {
		FilenetHelper filenet = caller.getFilenetHelper();
		ObjectStore os = filenet.os;
		System.out.println(""+os);
		ClassDescriptionSet cds = os.get_ClassDescriptions();
		PageIterator pi = cds.pageIterator();
		HashMap<String,Object> mainlist = new HashMap<String,Object>();
		while(pi.nextPage()) {
			Object page[] = pi.getCurrentPage();
			for(int i=0; i<page.length; i++) {
				ClassDescription cd = (ClassDescription) page[i];
				System.out.println(cd.get_SymbolicName());
				HashMap<String, Object> cdmap = new HashMap<String, Object>();
				mainlist.put(cd.get_SymbolicName(), cdmap);
				cdmap.put("id", cd.get_Id().toString());
				cdmap.put("name", cd.get_Name());
				cdmap.put("description", cd.get_DescriptiveText());
				ClassDescription s = cd.get_SuperclassDescription();
				if (s!=null) {
					cdmap.put("super", s.get_SymbolicName());
					cdmap.put("superId", s.get_Id().toString());
				}
				HashMap<String,Object> props = new HashMap<String,Object>();
				cdmap.put("properties", props);
				PropertyDescriptionList pdl = cd.get_PropertyDescriptions();
				Iterator it2 = pdl.iterator();
				while(it2.hasNext()) {
					PropertyDescription pd = (PropertyDescription) it2.next();
					HashMap<String, Object> prop = new HashMap<String, Object>();
					props.put(pd.get_SymbolicName(), prop);

					prop.put("id", pd.get_Id().toString());
					prop.put("name", pd.get_Name());
					prop.put("description", pd.get_DescriptiveText());
					prop.put("type", pd.get_DataType().toString());
				}				
			}
		}
		new FileHelper().saveJsonObject("descriptors.json", mainlist);

	}
	
	public static void startSimulation(String docClass) {
		try {
			Connection connection = DBConnectionFactory.defaultFactory.getConnection("startSimulation");
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
			DBConnectionFactory.close(connection);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
