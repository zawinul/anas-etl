package it.eng.anas.threads;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

import it.eng.anas.QueueManager;
import it.eng.anas.Utils;
import it.eng.anas.model.Esempio1;
import it.eng.anas.model.Model;

public class QueueConsumeJob<T extends Model> extends Job {
	public QueueManager manager;
	public String queueName;
	public String host = "localhost";
	public String tag = "localhost";
	
	public boolean closed = false;
	public Class<? extends Model> tclass;
	public ObjectMapper mapper;
	protected Channel channel = null;
	protected Connection connection = null;
	public int waitOnBusy = 500;
	
	public void log(String msg) {
		System.out.println(position+":"+tag+":"+msg);
	}

	public QueueConsumeJob(String tag, String queueName, String type, int priority, Class<? extends Model> tclass) {

		super(tag, type, priority);
		this.queueName = queueName;
		this.tclass = tclass;
	}
	
	public void execute() throws Exception {
		log("start");
		mapper = Utils.getMapper();
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host);
		connection = factory.newConnection();	
		channel = connection.createChannel();
		
		boolean durable = true;
		boolean exclusive = false;
		boolean autoDelete = false;
		channel.queueDeclare(queueName, durable, exclusive, autoDelete, null);

		while(true) {
			if (exitRequest)
				break;
			boolean d = singleStep();
			if (!d) {
				// probabilmente sono finiti i dati in coda
				// aspettiamo un po' prima di uscire e creare un altro thread
				Utils.sleep(2000);
				break;
			}
		}
		close();
		log("end");
	}

	public void onMessage(T obj) throws Exception {
		log("onMessage "+obj);
		log(channel.messageCount(queueName)+" remaining");
		int t = 500+(int)(Math.random()*5000);
		Utils.sleep(t);
	}

	public T beforeResend(T obj) {
		((Esempio1)obj).nretry++;
		return obj;
	}

	private boolean singleStep() throws Exception {
		GetResponse resp;
		resp = channel.basicGet(queueName, false);
		if (resp==null) {
			log("deq thread "+queueName+" empty queue");
			return false;
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
		return true;
		
	}

	public void close()  {

		log("close");
		Channel ch = channel;
		Connection con = connection;
		
		channel = null;
		connection = null;
		
		try {
			if (ch!=null)
				ch.close();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		try {
			if (con!=null)
				con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		closed = true;
	}	

}
