package it.eng.anas.etl;

import it.eng.anas.FileHelper;
import it.eng.anas.Utils;
import it.eng.anas.model.DBJob;

public class AnasEtlJob extends DBConsumeJob {

	private FileHelper fileHelper = new FileHelper();

	public AnasEtlJob(String tag, String queueName, int priority) {
		super(tag, queueName, priority);
	}

	@Override
	public void onMessage(DBJob job) throws Exception {
		log("AnasEtlJob onMessage "+tag+" "+job);
		String path = Utils.rndString(1)+"/"+Utils.rndString(1)+"/"+Utils.rndString(1)+"/"+Utils.rndString(10)+".json";
		log("path="+path);
		fileHelper.saveJsonObject(path, job);

		Utils.randomSleep(1000, 5000);
	}

}
