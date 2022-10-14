package it.eng.anas.main;

import it.eng.anas.QueueConsumer;
import it.eng.anas.Utils;
import it.eng.anas.model.Esempio1;

public class MainReceive {

	public static void main(String[] args) throws Exception {
		 
		QueueConsumer<Esempio1> consumer = new QueueConsumer<Esempio1>("localhost", "esempio1", Esempio1.class) {

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

		consumer.waitOnBusy = 10;
		System.out.println("wait...");
		
		
		Utils.sleep(200);
		consumer.start();
		System.out.println("started...");
		consumer.waitForExit();
		System.out.println("after waitForExit");

		consumer.close();
		
	}

	
}
