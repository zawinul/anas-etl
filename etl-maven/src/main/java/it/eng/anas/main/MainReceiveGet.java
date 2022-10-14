package it.eng.anas.main;

import it.eng.anas.QueueConsumerGet;
import it.eng.anas.Utils;
import it.eng.anas.model.Esempio1;

public class MainReceiveGet {

	public static void main(String[] args) throws Exception {

		QueueConsumerGet<Esempio1> consumer = new QueueConsumerGet<Esempio1>("localhost", "esempio1", Esempio1.class) {

			@Override			
			public void onMessage(Esempio1 obj) throws Exception {
				log("onMessage "+obj);
				log(getChannel().messageCount(queueName)+" remaining");
			}
			
			@Override			
			public Esempio1 beforeResend(Esempio1 obj) {
				obj.nretry++;
				return obj;
			}
		};
		consumer.start();
		consumer.waitOnBusy = 10;
		consumer.start();
		System.out.println("wait...");
		Utils.sleep(3000);
		consumer.enabled = true;
		System.out.println("enabled...");
		Utils.sleep(60000*5);
		System.out.println("after wait");
		consumer.exit = true;
		System.out.println("wait for thread exit");
		consumer.thread.join(0);
		consumer.close();
		
	}

	
}
