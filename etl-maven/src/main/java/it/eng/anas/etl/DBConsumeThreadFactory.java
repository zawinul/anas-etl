package it.eng.anas.etl;

import java.util.List;

import it.eng.anas.FileHelper;
import it.eng.anas.Utils;
import it.eng.anas.model.Esempio1;
import it.eng.anas.threads.Job;
import it.eng.anas.threads.JobFactory;

public class DBConsumeThreadFactory extends JobFactory {
	
	public Job create(List<Job> currentJobs) {
		String tag = Utils.rndString(5);
		String queue = "esempio1";
		FileHelper fileHelper = new FileHelper();
		

		return new DBConsumeJob<Esempio1>(tag, "pippoqueue", "estrai-meta", 5, Esempio1.class) {

			@Override
			public void onMessage(Esempio1 obj) throws Exception {
				log("DBConsumeThreadFactory onMessage "+obj);
				String path = Utils.rndString(1)+"/"+Utils.rndString(1)+"/"+Utils.rndString(1)+"/"+Utils.rndString(10)+".json";
				log("path="+path);
				fileHelper.saveJsonObject(path, obj);

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
