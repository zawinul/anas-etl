package it.eng.anas.monitor;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLConnection;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.eclipse.jetty.websocket.api.*;
import org.eclipse.jetty.websocket.api.annotations.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import it.eng.anas.Event;
import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.db.DBConnectionFactory;
import it.eng.anas.db.DbJobManager;
import it.eng.anas.db.DbJobManagerTransactional;
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
			spark.Spark.webSocket("/monitor", EtlMonitorWebSocket.class);
		
		spark.Spark.port(port);
		spark.Spark.staticFileLocation("/web");
		spark.Spark.before((request, response) -> {
	        response.header("Access-Control-Allow-Origin", request.headers("Origin"));
	        //response.header("Access-Control-Request-Method", methods);
	        //response.header("Access-Control-Allow-Headers", headers);
	        // Note: this may or may not be necessary in your particular application
	        //response.type("application/json");
	    });
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

		spark.Spark.get("/getdir", (req, res) -> {
			String path = req.queryParams("path");
			if (path==null)
				path="/";
			String ret[][] = getDir(path);
			return Utils.getMapper().writeValueAsString(ret);
		});

		spark.Spark.get("/getdir2", (req, res) -> {
			try {
				String path = req.queryParams("path");
				if (path==null)
					path="/";
				String ret[][] = getDir2(path);
				return Utils.getMapper().writeValueAsString(ret);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				Log.log(e);
				throw e;
			}
		});

		spark.Spark.get("/getfile", (req, res) -> {
			String path = req.queryParams("path");
			if (path==null)
				path="/";
			File f = getFile(path);
			String mimeType = URLConnection.guessContentTypeFromName(f.getName());
			res.header("Content-Type", mimeType);
			return new FileInputStream(f);
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
	public static class EtlMonitorWebSocket {


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
    
    private String [][] getDir(String path) throws Exception {
    	List<String> f = new ArrayList<>();
    	List<String> d = new ArrayList<>();
    	String base = Utils.getConfig().outputBasePath;
    	File baseFile = new File(base);
    	File dir = new File(base, path);
    	if (!dir.exists() || !dir.isDirectory())
    		throw new Exception ("dir "+dir.getAbsolutePath()+" does not exist");
    	File files[] = dir.listFiles(); 
    	for(File el: files) {
    		if (el.exists() && el.isDirectory())
    			d.add(el.getName());
    		if (el.exists() && !el.isDirectory())
    			f.add(el.getName());
    	}
    	String darr[] = d.toArray(new String[0]);
    	String farr[] = f.toArray(new String[0]);
    	String [][] ret = {darr, farr};
    	return ret;
    }

    
    private String [][] getDir2(String path) throws Exception {
    	List<String> f = new ArrayList<>();
    	List<String> d = new ArrayList<>();
    	String base = Utils.getConfig().outputBasePath;
    	File baseFile = new File(base);
    	File dir = new File(base, path);
    	if (!dir.exists() || !dir.isDirectory())
    		throw new Exception ("dir "+dir.getAbsolutePath()+" does not exist");
    	File files[] = dir.listFiles(); 
    	for(File el: files) {
    		if (el.exists() && el.isDirectory())
    			d.add(el.getName()); 
    		if (el.exists() && !el.isDirectory())
    			f.add(getFileTitle(el));
    	}
    	String darr[] = d.toArray(new String[0]);
    	String farr[] = f.toArray(new String[0]);
    	String [][] ret = {darr, farr};
    	return ret;
    } 
    
    private String getFileTitle(File f) throws Exception {
    	String name = f.getName();
    	if (!name.endsWith(".json"))
    		return name+":";
    	JsonNode node = Utils.getMapper().readTree(f);
    	if (node==null)
    		return name+":";
    	if (!(node instanceof ObjectNode))
    		return name+":";
    	JsonNode titolo = node.get("titolo");
    	return (titolo==null) ? name+":" : name+":" + titolo.asText();
    }

    private File getFile(String path) throws Exception {
    	String base = Utils.getConfig().outputBasePath;
    	File baseFile = new File(base);
    	File ret = new File(base, path);
    	if (!ret.exists() || !ret.isFile())
    		throw new Exception("file "+path+" does not exists");
    	return ret;
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
	
    public static void main(String args[]) throws Exception {
    	WebServer w = new WebServer();
    	String d[][] = w.getDir("/");
    	HashMap<String, Object> k = new HashMap<>();
    	k.put("directories", d[0]);
    	k.put("files", d[1]);
    	System.out.println(Utils.getMapper().writeValueAsString(k));
    }
}
