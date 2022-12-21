package it.eng.anas;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log {
	
	private static Logger logger = LoggerFactory.getLogger("etl");
	private static String configFile;

	static {
		Event.addListener("config-change", new Runnable() {
			
			public void run() {
				String lf = Utils.getConfig().logConfigFile;
				if (lf!=null) {
					if (!lf.equals(configFile)) {
						configFile = lf;
						PropertyConfigurator.configure(lf);
						logger = LoggerFactory.getLogger("etl");
						log("log reconfigured from file "+lf);
					}
				}
			}
		});
	}
	
	

	public static void log(String x) {
		logger.info(x);
	}

	public static void warn(String x) {
		logger.warn(x);
	}


	public static void log(Exception e) {
		e.printStackTrace();
		logger.error(e.getMessage(), e);
	}

}
