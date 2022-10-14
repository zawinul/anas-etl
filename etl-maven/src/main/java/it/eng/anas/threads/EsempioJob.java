package it.eng.anas.threads;

import it.eng.anas.Utils;

public class EsempioJob extends Job {
	public EsempioJob(String tag, String type, int priority) {
		super(tag, type, priority);
	}
	
	public void execute() throws Exception {
		log("start");
		while(true) {
			if (exitRequest)
				break;
			if (Math.random()<.03)
				throw new Exception("che ci vuoi fare, succede");
			log("in loop");
			int t = 5000+ (int) (Math.random()*20000);
			Utils.sleep(t);
		}
		log("end");
	}


}
