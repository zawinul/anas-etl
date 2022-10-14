package it.eng.anas.main;

import it.eng.anas.QueueProducer;
import it.eng.anas.model.Esempio1;

public class MainSend {

	public static void main(String[] args) throws Exception {
		QueueProducer<Esempio1> producer = new QueueProducer<Esempio1>("localhost", "esempio1", Esempio1.class);
		for(int i=0; i<5000; i++) {
			Esempio1 campione = new Esempio1();
			producer.send(campione);
		}
		producer.close();
		System.out.println("ok");
	}

}
