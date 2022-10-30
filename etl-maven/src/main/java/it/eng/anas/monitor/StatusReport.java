package it.eng.anas.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.protobuf.Internal.ListAdapter;

import it.eng.anas.Utils;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.ResultSetToJson;
import it.eng.anas.etl.AnasEtlWorker;
import it.eng.anas.etl.DBConsumeWorker;
import it.eng.anas.threads.ThreadManager;
import it.eng.anas.threads.Worker;

public class StatusReport {

	private ResultSetToJson js = new  ResultSetToJson();
	
	private final String query[] = {
		"SELECT count(*) as count FROM job",	
		"SELECT count(*) as count FROM job_done",	
		"SELECT count(*) as count FROM job_error",	
		"SELECT queue,status,nretry,count(*) as count FROM job group by queue,nretry,status"	
	};
	
	
	public String getJobs() throws Exception {
		HashMap<String,Object> main = new HashMap<String,Object>();
		ArrayList<String> descs = new ArrayList<String>();
		
		ArrayList<Object> list = new ArrayList<Object>();
		if (ThreadManager.mainThreadManager!=null) {
			for(Worker w: ThreadManager.mainThreadManager.threads) {
				descs.add(w.curJobDescription);
				if (w instanceof DBConsumeWorker)
					list.add(((DBConsumeWorker)w).currentJob);
				else
					list.add(w.curJobDescription);
			}
		}
		main.put("descriptions",  descs);
		main.put("jobs", list);
		return Utils.getMapper().writeValueAsString(main);
	}
	
	public String getReport() throws Exception {
		
		ObjectMapper mapper = Utils.getMapper();
		SimpleModule module = new SimpleModule();
		module.addSerializer(AnasEtlWorker.class, new WorkerSerializer());
		mapper.registerModule(module);
			
		ObjectNode main = mapper.createObjectNode();
		main.put("openConnections",  DBConnectionFactory.nopen);
		if (ThreadManager.mainThreadManager!=null) {
			main.put("forcedNumberOfThreads", ThreadManager.mainThreadManager.forcedNumberOfThreads);
			main.set("workers", mapper.valueToTree(ThreadManager.mainThreadManager.threads));
		}
		for(String sql: query) {
			ArrayNode data = js.extract(sql);
			main.set(sql, data);
		}
		return mapper.writeValueAsString(main);
	}
	
	public static class WorkerSerializer extends StdSerializer<AnasEtlWorker> {
	    
	    public WorkerSerializer() {
	        this(null);
	    }
	  
	    public WorkerSerializer(Class<AnasEtlWorker> t) {
	        super(t);
	    }

	    @Override
	    public void serialize(
	    		AnasEtlWorker value, JsonGenerator jgen, SerializerProvider provider) 
	      throws IOException, JsonProcessingException {
	 
	        jgen.writeStartObject();
	        jgen.writeStringField("jobDescription", value.curJobDescription);
	        jgen.writeObjectField("job", value.currentJob);
	        jgen.writeNumberField("priority", value.priority);
	        jgen.writeStringField("tag", value.tag);
	        jgen.writeStringField("queue", value.queueName);
	        jgen.writeBooleanField("closed", value.closed);
	        jgen.writeEndObject();
	    }
	}
	

	public static void main(String args[]) throws Exception {
		System.out.println(new StatusReport().getReport());
	}
}
