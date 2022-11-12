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
	public Integer maxrecursion = null;
	public Boolean withdoc = null;
	public Boolean withcontent = null;
	

	
	public AnasEtlJob withFolderId(String folderId) {
		this.folderId = folderId;
		return this;
	}
	public AnasEtlJob withPath(String path) {
		this.path = path;
		return this;
	}
	public AnasEtlJob withMaxrecursion(Integer maxrecursion) {
		this.maxrecursion = maxrecursion;
		return this;
	}
	public AnasEtlJob withPriority(Integer priority) {
		this.priority = priority;
		return this;
	}
	public AnasEtlJob withNretry(Integer nretry) {
		this.nretry = nretry;
		return this;
	}
	public AnasEtlJob withOperation(String operation) {
		this.operation = operation;
		return this;
	}
	public AnasEtlJob withDocId(String docId) {
		this.docId = docId;
		return this;
	}
//	public AnasEtlJob withKey1(String key1) {
//		this.key1 = key1;
//		return this;
//	}
//	public AnasEtlJob withKey2(String key2) {
//		this.key2 = key2;
//		return this;
//	}
//	public AnasEtlJob withKey3(String key3) {
//		this.key3 = key3;
//		return this;
//	}
	
	public AnasEtlJob withKeys(String key1, String key2, String key3) {
		this.key1 = key1;
		this.key2 = key2;
		this.key3 = key3;
		return this;
	}
	
	public AnasEtlJob withParentJob(int parent_job) {
		this.parent_job = parent_job;
		return this;
	}

	
	private static ObjectMapper mapper = Utils.getMapperOneLine();
	public static AnasEtlJob clone(AnasEtlJob job) {
		try {
			JsonNode node = mapper.valueToTree(job);
			return mapper.treeToValue(node, AnasEtlJob.class);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}