package it.eng.anas.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import it.eng.anas.Utils;

public class DBJob extends Model {	
	public int id;
	public String objectid;
	public Integer priority;
	public Status status;
	public String queue;
	public String creation;
	public String last_change;
	public String command;
	public String body;
	
	public static enum Status {
		ready,
		process,
		done,
		error
	}
	
	public String toString() {
		try {
			if (body==null) 
				return null; 
			ObjectMapper mapper = Utils.getMapperOneLine();
			ObjectNode node = (ObjectNode) mapper.readTree(body);
			ObjectNode main = (ObjectNode) mapper.readTree(mapper.writeValueAsString(this));
			main.set("body", node);
			return main.asText();
		}
		catch(Exception e) {
			return super.toString();
		}
	}
}
