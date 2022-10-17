package it.eng.anas.etl;

import java.util.List;

import it.eng.anas.FileHelper;
import it.eng.anas.Utils;
import it.eng.anas.model.DBJob;
import it.eng.anas.threads.Job;
import it.eng.anas.threads.JobFactory;

public class DBConsumeThreadFactory extends JobFactory {
	
	public Job create(List<Job> currentJobs) {
		String tag = Utils.rndString(5);
		String queue = "esempio1";
		FileHelper fileHelper = new FileHelper();
		

		return new DBConsumeJob(tag, "pippo", 5) {

			@Override
			public void onMessage(DBJob job) throws Exception {
				log("DBConsumeThreadFactory onMessage "+job);
				String path = Utils.rndString(1)+"/"+Utils.rndString(1)+"/"+Utils.rndString(1)+"/"+Utils.rndString(10)+".json";
				log("path="+path);
				fileHelper.saveJsonObject(path, job);

				Utils.randomSleep(1000, 5000);
			}



		};
	}
}
