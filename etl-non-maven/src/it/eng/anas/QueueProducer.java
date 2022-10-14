package it.eng.anas;

import com.rabbitmq.client.MessageProperties;

import it.eng.anas.model.Model;
import it.eng.anas.model.QueueModel;

public class QueueProducer<T extends Model> extends QueueManager {
	
	public Class<? extends Model> objClass;
	public QueueProducer() {
		super();
	}
	
	public QueueProducer(String host, String queueName, Class<?extends QueueModel> clss) {
		super(host, queueName);
		objClass = clss;
	}

	public void send(T obj) throws Exception {
		String json = mapper.writeValueAsString(obj);
		getChannel().basicPublish("", queueName,  MessageProperties.PERSISTENT_TEXT_PLAIN, json.getBytes());
		log("sent '" + obj + "'");
	}	
}
