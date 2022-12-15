package it.eng.anas.etl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Utils;
import it.eng.anas.model.DBJob;

public class AnasEtlJob extends DBJob {
	public String os = null;
	public String folderId;
	public String docId;
	public String path = null;
	public Boolean withdoc = null;
	public Boolean withcontent = null;
	public Boolean buildDir = false;
	

	private static ObjectMapper mapper = Utils.getMapperOneLine();
	private static AnasEtlJob clone(AnasEtlJob job) {
		try {
			JsonNode node = mapper.valueToTree(job);
			return mapper.treeToValue(node, AnasEtlJob.class);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static AnasEtlJob createSubJob(AnasEtlJob job) {
		AnasEtlJob subjob = clone(job);
		job.parent_job = job.id;
		return subjob;
	}

}