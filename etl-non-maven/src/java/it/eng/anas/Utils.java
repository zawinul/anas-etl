package it.eng.anas;

import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import it.eng.anas.model.Config;

public class Utils {
	public static String rndString(int len) {
		char l[]=new char[len];
		for(int i=0; i<len;i++) 
			l[i] = (char) ('A'+Math.random()*25);
		return new String(l);
	}
	
	public static void sleep(int ms) {
		_sleep(ms);
	}
	
	public static void randomSleep(int msMin, int msMax) {
		sleep(msMin+(int)(Math.random()*(msMax-msMin)));
	}
	
	public static void shortPause() {
		Config c = getConfig();
		randomSleep(c.shortPause[0], c.shortPause[1]);
	}

	
	public static void longPause() {
		Config c = getConfig();
		randomSleep(c.longPause[0], c.longPause[1]);
	}


	private static void _sleep(int ms)  {		
		while(ms>0) {
			if(exiting)
				break;
			int t = ms>1000 ? 1000 : ms;
			ms -= t;
			try {
				Thread.sleep(t);
			} catch (InterruptedException e) {
				//e.printStackTrace();
			}
		}
	}

	public static ObjectMapper getMapper() {
		ObjectMapper mapper = getMapperOneLine();
		mapper.enable(SerializationFeature.INDENT_OUTPUT);
		return mapper;
	}

	public static String getStackTrace(Exception e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw, true);
		e.printStackTrace(pw);
		pw.close();
		String s = sw.getBuffer().toString();
		return e.getMessage()+"\n"+s;
	}
	
	public static ObjectMapper getMapperOneLine() {
		ObjectMapper mapper = new ObjectMapper()
				.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
				.configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, false)
				.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
				.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
				.configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, false)
				.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false)
				.configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false)
				.configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, false)
				.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false)
				.configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, false)
				.configure(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY, false)
				.configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, false)
				.configure(DeserializationFeature.FAIL_ON_UNRESOLVED_OBJECT_IDS, false)
			;
		return mapper;
	}
	
	
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssS");
	private static String datePattern = dateFormat.format(new Date(0L));
	private static int dpLen = datePattern.length();

	public static String date2String(Date d) {
		if (d==null)
			return "null";
		else
			return dateFormat.format(d);
	}

	public static Date string2Date(String s) {
		if (s==null)
			return null;
		int len = s.length();
		if (len > dpLen)
			s = s.substring(0, dpLen);
		else if (len < dpLen)
			s = s+datePattern.substring(len);
		try {
			return dateFormat.parse(s);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private static Config cfg;
	private static String jsonCfg;

	public static Config getConfig() {
		return (cfg!=null) ? cfg : refreshConfig();
	}
	
	private static Config refreshConfig()  {
		try {
			FileReader r = new FileReader(Global.configFile);
			String json = IOUtils.toString(r);
			if (!json.equals(jsonCfg)) {
				jsonCfg = json;
				Log.etl.log("Config changed: "+json);
				cfg = getMapper().readValue(json, Config.class);
				
				if (new File("./password.filenet").exists())
					cfg.filenet.password = IOUtils.toString(new FileReader("./password.filenet"));
				if (new File("./password.db").exists())
					cfg.db.password = IOUtils.toString(new FileReader("./password.db"));
				Event.emit("config-change");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return cfg;
	}
	
	private static Thread refreshConfigThread;
	private static boolean exiting = false;
	
	static {
		Event.addListener("exit", new Runnable() {
			public void run() {
				exiting=true;
			}
		});
		refreshConfigThread = new Thread() {
			public void run() {
				while(!exiting) {
					Utils.sleep(30000);
					refreshConfig();
				}
			}
		};
		refreshConfigThread.start();
	}
}
