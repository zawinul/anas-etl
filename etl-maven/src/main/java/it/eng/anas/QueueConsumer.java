package it.eng.anas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import it.eng.anas.model.Model;

public class QueueConsumer<T extends Model> extends QueueManager  {
	public int index = 0;
	public boolean enabled = false;
	public boolean exit = false;
	public int waitOnIdle = 10000;
	public int waitOnBusy = 500;
	public int waitOnNoChannel = 30000;

	public String nodeDesc = "node1";

	public Class<? extends Model> tclass = null;
	public String tag = Utils.rndString(5);
	boolean terminated = false;
	WaitForTerminationThread tthread;

	public void waitForExit() {
		try {
			if (tthread!=null)
				tthread.join();
			close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void log(String msg) {
		System.out.println(nodeDesc+"|"+tag+"|"+msg);
	}
	
	public QueueConsumer() {
	}
	
	public QueueConsumer(String host, String queueName, Class<? extends T> tclass) {
		super(host, queueName);
		this.tclass = tclass;
	}
	
	
	public class RabbitConsumer implements Consumer {
		
		public void handleConsumeOk(String consumerTag) {
			log("handleConsumeOk");
		}

		public void handleCancelOk(String consumerTag) {
			log("handleCancelOk");
			terminated = true;
		}

		public void handleCancel(String consumerTag) throws IOException {
			log("handleCancel");
			terminated = true;
		}

		public void handleDelivery(String arg0, Envelope arg1, BasicProperties arg2, byte[] arg3) throws IOException {
			Channel channel = getChannel();
			String body = new String(arg3, StandardCharsets.UTF_8);
			log("onMessage|"+body);
			@SuppressWarnings("unchecked")
			T obj = (T) mapper.readValue(body, tclass);
			
			try {
				onMessage(obj);
				log("ok");
				boolean autoDeleteMessage = true;
				channel.basicAck(arg1.getDeliveryTag(), autoDeleteMessage);
				Utils.sleep(700);
			} 
			catch (Exception e) {
				log("error on receive BL: "+e.getMessage());
				e.printStackTrace();
				boolean requeue = false;
				boolean multiple = false;
				channel.basicNack(arg1.getDeliveryTag(), multiple, requeue);
				
				Utils.sleep(1000);
				// transform and resend
				T transformed = beforeResend(obj);
				if (transformed != null) {
					String json = mapper.writeValueAsString(transformed);
					channel.basicPublish("", queueName, null, json.getBytes());
				}
			}
		}

		public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
			log("handleShutdownSignal");
		}

		public void handleRecoverOk(String consumerTag) {
			log("handleRecoverOk");
		}

	};
	
	public void start() {
		try {
			log("Waiting for messages");
			Channel channel = getChannel();
			if (channel!=null) {
				boolean autoAck = false;
				RabbitConsumer consumer = new RabbitConsumer();
				if (channel.messageCount(queueName)>0) {
					tthread = new WaitForTerminationThread(this);
					tthread.start();
					channel.basicConsume(queueName, autoAck, tag,consumer);
				}
			}
			System.out.println("end of start");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private boolean stopped = false;
	public void stop() throws Exception  {
		System.out.println("stop");
		if (!stopped) {
			try {
				getChannel().basicCancel(tag);
			} catch (IOException e) {
				System.out.println(e.getMessage());
			}
			stopped = true;
		}
	}
	
	
	public void onMessage(T obj) throws Exception {
	}
	
	public T beforeResend(T obj) {
		return obj;
	}


	public static class WaitForTerminationThread extends Thread {
		QueueConsumer<? extends Model> consumer;
		public WaitForTerminationThread(QueueConsumer<? extends Model> qc) {
			consumer = qc;
		}
		
		@Override
		public void run() {
			while(!consumer.closed) 
				Utils.sleep(1000);
			
			System.out.println("end of WaitForTerminationThread");
		}
	}
}
