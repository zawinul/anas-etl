package it.eng.anas.monitor;


import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.eclipse.jetty.websocket.api.*;
import org.eclipse.jetty.websocket.api.annotations.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import it.eng.anas.Event;
import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.db.ResultSetToJson;
import it.eng.anas.db.SimpleDbOp;
import it.eng.anas.etl.AnasEtlJob;
import it.eng.anas.etl.AnasEtlWorker;
import it.eng.anas.model.Config;
import it.eng.anas.monitor.StatusReport.WorkerSerializer;
import it.eng.anas.threads.ThreadManager;

public class WebServer {

	public WebServer() {
	}

	public Runnable onKill;
	public void start() {
		Config c = Utils.getConfig();
		int port = c.webServerPort;

		if (c.websocketEnabled)
			spark.Spark.webSocket("/monitor", EchoWebSocket.class);
		
		spark.Spark.port(port);
		spark.Spark.staticFileLocation("/web");
		spark.Spark.get("/hello", (req, res) -> {
			Log.log(req.url().toString());
			
			return "Hello World "+Math.random()+ " "+req.url();
		});

		spark.Spark.get("/setn/:n", (req, res) -> {
			Log.log(req.url().toString());
			int n = Integer.parseInt(req.params("n"));
			ThreadManager.mainThreadManager.setNumberOfThreads(n);
			return "Hello World n="+n+ " "+req.url();
		});

		spark.Spark.get("/threads", (req, res) -> {
			try {
				Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
				FileWriter w = new FileWriter("./thread-dump.txt");
				for(Thread t:map.keySet()) {
					w.write(t+"\n");
					StackTraceElement[] tr = map.get(t);
					for(StackTraceElement element:tr) {
						w.write("\t"+element.toString()+"\n");						
					}
					Log.log(t+"\n");
				}
				w.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			return "ok";
		});
		spark.Spark.get("/report", (req, res) -> {
			try {
				return new StatusReport().getReport();
			} catch (Exception e) {
				e.printStackTrace();
				return e.getMessage();
			}
		});

		spark.Spark.post("/insertJob", (req, res) -> {
			DbJobManager<AnasEtlJob> manager = new DbJobManager<AnasEtlJob>("insertJob", AnasEtlJob.class);
			String json = req.body();
			AnasEtlJob input = Utils.getMapper().readValue(json, AnasEtlJob.class);
			AnasEtlJob job = manager.insertNew(input);
			manager.close();
			return Utils.getMapper().writeValueAsString(job);
		});

		spark.Spark.get("/jobs", (req, res) -> {
			return new StatusReport().getJobs();
		});

		spark.Spark.get("/start/:class", (req, res) -> {
			String docClass = req.params("class");
			Event.emit("start-simulation", docClass);
			return "ok";
		});


		spark.Spark.post("/sql", (req, res) -> {
			try {
				String query = req.body();
				Connection conn = DBConnectionFactory.defaultFactory.getConnection("web");
				SimpleDbOp op = new SimpleDbOp(conn)
					.query(query)
					.executeQuery();
				ResultSetToJson js = new  ResultSetToJson();
				ArrayNode extract = js.extract(op.getResultSet());
				op.close();
				DBConnectionFactory.close(conn);
				return Utils.getMapper().writeValueAsString(extract);
			} catch (Exception e) {
				e.printStackTrace();
				return e.getMessage();
			}
		});

		spark.Spark.get("/exit", (req, res) -> {
			Event.emit("exit");
			return "exit launched";
		});
		
		spark.Spark.init();
//		(new Thread() {
//			public void run() {
//				for(int i=0; i<100;i++) {
//					Utils.sleep(5000);
//					sendToCLient("aaa 111 "+i);
//				}
//			}
//		}).start();
		
		StatusReport.init();
	}

    // Store sessions if you want to, for example, broadcast a message to all users
    public  static final Queue<Session> sessions = new ConcurrentLinkedQueue<>();
	
    @WebSocket
	public static class EchoWebSocket {


	    @OnWebSocketConnect
	    public void connected(Session session) {
	        sessions.add(session);
	    }

	    @OnWebSocketClose
	    public void closed(Session session, int statusCode, String reason) {
	        sessions.remove(session);
	    }

	    @OnWebSocketMessage
	    public void message(Session session, String message) throws IOException {
	        System.out.println("Got: " + message);   // Print message
	        //session.getRemote().sendString(message); // and send it back
	    }
	}
    
    private static ObjectMapper webSocketMapper = Utils.getMapper();
    static {
    	webSocketMapper = Utils.getMapper();
		SimpleModule module = new SimpleModule();
		module.addSerializer(AnasEtlWorker.class, new WorkerSerializer());
		webSocketMapper.registerModule(module);

    }
	
    public static void sendToClient(String x, Object obj)  {
    	for(Session session: sessions) {
    		try {
    			ObjectNode n = webSocketMapper.createObjectNode();
    			n.set(x,  webSocketMapper.valueToTree(obj));
    			String json = webSocketMapper.writeValueAsString(n);
    			if (session.isOpen())
    				session.getRemote().sendString(json);
			} catch (Exception e) {
				Log.log("Websocket error "+e.getMessage());
				//e.printStackTrace();
			}
    	}
    }
	
}
