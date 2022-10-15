package it.eng.anas.threads;

import java.util.List;

import it.eng.anas.Utils;
import it.eng.anas.model.Esempio1;

public class DBConsumeThreadFactory extends JobFactory {
	
	public Job create(List<Job> currentJobs) {
		String tag = Utils.rndString(5);
		String queue = "esempio1";
		
		return new DBConsumeJob<Esempio1>(tag, "pippoqueue", "estrai-meta", 5, Esempio1.class) {

			@Override
			public void onMessage(Esempio1 obj) throws Exception {
				log("DBConsumeThreadFactory onMessage "+obj);
				Utils.randomSleep(1000, 5000);
			}

			@Override
			public Esempio1 beforeResend(Esempio1 obj) {
				obj.nretry++;
				return obj;
			}

		};
	}
}
