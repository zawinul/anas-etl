package it.eng.anas.monitor;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Consumer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import it.eng.anas.Event;
import it.eng.anas.Log;
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
		"job", "SELECT count(*) as count FROM job",	
		"done", "SELECT count(*) as count FROM job_done",	
		"error", "SELECT count(*) as count FROM job_error",	
		"operation",       "SELECT queue,operation,count(*) as count FROM job       group by queue,operation order by queue, operation",	
		"done_operation",  "SELECT queue,operation,count(*) as count FROM job_done  group by queue,operation order by queue, operation",	
		"error_operation", "SELECT queue,operation,count(*) as count FROM job_error group by queue,operation order by queue, operation",	
		//"status", "SELECT queue,locktag,count(*) as count FROM job group by queue,locktag",	
		"retry", "SELECT queue,nretry,count(*) as count FROM job group by queue,nretry"	
	};
	
	public static void init () {
		Event.addListener("worker-changed", new Consumer<Object>() {
			public void accept(Object obj) {
				Worker worker = (Worker) obj;
				WebServer.sendToClient("update-worker", worker);
			}
		});
	}
	
	public String getJobs() throws Exception {
		HashMap<String,Object> main = new HashMap<String,Object>();
		ArrayList<String> descs = new ArrayList<String>();
		
		ArrayList<Object> list = new ArrayList<Object>();
		if (ThreadManager.mainThreadManager!=null) {
			for(Worker w: ThreadManager.mainThreadManager.threads) {
				if (w instanceof DBConsumeWorker)
					list.add(((DBConsumeWorker)w).currentJob);
				else
					list.add(w.toString());
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
		Connection con = DBConnectionFactory.defaultFactory.getConnection("getReport");
		ObjectNode main = mapper.createObjectNode();
		main.put("openConnections",  DBConnectionFactory.nopen);
		//main.put("queue",  Utils.getConfig().processingQueue);
		if (ThreadManager.mainThreadManager!=null) {
			main.put("forcedNumberOfThreads", ThreadManager.mainThreadManager.forcedNumberOfThreads);
			main.set("workers", mapper.valueToTree(ThreadManager.mainThreadManager.threads));
		}
		for(var i=0; i<query.length; i+=2) {
			ArrayNode data = js.extract(query[i+1], con);
			main.set(query[i], data);
		}
		DBConnectionFactory.close(con);
		String connections[] = DBConnectionFactory.getOpenConnectors();
		main.set("connections", mapper.valueToTree(connections));
		return mapper.writeValueAsString(main);
	}
	
	public static class WorkerSerializer extends StdSerializer<AnasEtlWorker> {
	    

		private static final long serialVersionUID = 1L;

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
	        jgen.writeObjectField("job", value.currentJob);
	        jgen.writeObjectField("index", value.index);
	        jgen.writeStringField("tag", value.tag);
	        jgen.writeStringField("status", value.workerStatus);
	        jgen.writeBooleanField("closed", value.closed);
	        jgen.writeEndObject();
	    }
	}
	

	public static void main(String args[]) throws Exception {
		Log.web.log(new StatusReport().getReport());
	}
}
