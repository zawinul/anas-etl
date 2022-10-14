package it.eng.anas.threads;

import java.util.List;

import it.eng.anas.Utils;
import it.eng.anas.main.MainGetQueues.QDesc;
import it.eng.anas.model.Esempio1;

public class QueueConsumeThreadFactory extends JobFactory {
	
	public Job create(List<Job> currentJobs) {
		String tag = Utils.rndString(5);
		String queue = "esempio1";
		
		return new QueueConsumeJob<Esempio1>(tag, "esempio1", "estrai-meta", 5, Esempio1.class) {
			@Override
			public void onMessage(Esempio1 obj) throws Exception {
				log("[] onMessage "+obj);
				log(channel.messageCount(queueName)+" remaining");
			}

			@Override
			public Esempio1 beforeResend(Esempio1 obj) {
				obj.nretry++;
				return obj;
			}

		};
	}
}
