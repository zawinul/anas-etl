package it.eng.anas;

import java.nio.charset.StandardCharsets;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;

import it.eng.anas.model.Model;

public class QueueConsumerGet<T extends Model> extends QueueManager implements Runnable {
	public int index = 0;
	public boolean enabled = false;
	public boolean exit = false;
	public int waitOnIdle = 10000;
	public int waitOnBusy = 500;
	public int waitOnNoChannel = 30000;

	public String nodeDesc = "node1";
	public String threadDesc = "AAA";

	public Thread thread = null;
	public Class<? extends Model> tclass = null;
	
	public void log(String msg) {
		System.out.println(nodeDesc+"|"+threadDesc+"|"+msg);
	}
	
	public QueueConsumerGet() {
	}
	
	public QueueConsumerGet(String host, String queueName, Class<? extends T> tclass) {
		super(host, queueName);
		this.tclass = tclass;
	}
	
	public void run() {
		log("Waiting for messages");

		while(true) {
			try {
				if (exit)
					return;
				if (enabled)
					singleStep();
				else 
					Utils.sleep(waitOnBusy);
			} 
			catch(Exception e) {
				log(e.getMessage());
				e.printStackTrace();
			}
		}

	}

	public void start() throws Exception {
		if (thread!=null) {
			//throw new Exception("duplicated start");
			log("duplicated start, ignored");
			return;
		}
		thread = new Thread(this);
		thread.start();
	}
	

	private void singleStep() throws Exception {
		Channel channel = getChannel();
		if (channel==null) {
			log("deq thread "+index+" waitOnNoChannel");
			Utils.sleep(waitOnNoChannel);
			return;
		}
		GetResponse resp;
		try {
			resp = channel.basicGet(queueName, false);
		} catch (Exception e1) {
			log(e1.getMessage());
			Utils.sleep(waitOnIdle);
			return;
		}
		if (resp==null) {
			log("deq thread "+index+" waitOnIdle");
			Utils.sleep(waitOnIdle);
			return;
		}
			
		String body = new String(resp.getBody(), StandardCharsets.UTF_8);
		log("onMessage|"+body);
		@SuppressWarnings("unchecked")
		T obj = (T) mapper.readValue(body, tclass);
		
		try {
			onMessage(obj);
			log("ok");
			boolean autoDeleteMessage = true;
			channel.basicAck(resp.getEnvelope().getDeliveryTag(), autoDeleteMessage);
		} catch (Exception e) {
			log("error on receive BL: "+e.getMessage());
			e.printStackTrace();
			boolean requeue = false;
			boolean multiple = false;
			channel.basicNack(resp.getEnvelope().getDeliveryTag(), multiple, requeue);
			
			// transform and resend
			T transformed = beforeResend(obj);
			if (transformed != null) {
				String json = mapper.writeValueAsString(transformed);
				channel.basicPublish("", queueName, null, json.getBytes());
			}
		}
		Utils.sleep(waitOnBusy);
		
	}
	
	public void onMessage(T obj) throws Exception {
	}
	
	public T beforeResend(T obj) {
		return obj;
	}


}
