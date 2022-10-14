package it.eng.anas;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
//import com.rabbitmq.client.ShutdownListener;
//import com.rabbitmq.client.ShutdownSignalException;

public class QueueManager  {
	public String host;	
	public String queueName;
	
	public boolean durable = true;
	public boolean exclusive = false;
	public boolean autoDelete = false;
	public boolean closed = false;

	public ObjectMapper mapper;
	
	protected Channel mainChannel = null;
	protected Connection connection = null;
	
	public QueueManager() {
		this("localhost", "noname");
	}
	
	public QueueManager(String host, String queueName) {
		this.host = host;
		this.queueName = queueName;
		mapper = Utils.getMapper();
	}
	
	
	@Override
	public void finalize() {
	    log("finalize");
        close();
	}
	
	public void log(String msg) {
		System.out.println("queueManager("+queueName+") "+msg);
	}
	

	public Channel getChannel() {
		if (mainChannel!=null)
			return mainChannel;
		
		try {
			if (connection==null) {
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost(host);
				connection = factory.newConnection();	
				
			}
		} 
		catch(Exception e) {
			e.printStackTrace();
			return null;
		}
		
		if (connection==null)
			return null;
		try {
			mainChannel = connection.createChannel();

//			int prefetchCount = 1;
//			mainChannel.basicQos(prefetchCount);

//			mainChannel.addShutdownListener(new ShutdownListener() {
//				
//				public void shutdownCompleted(ShutdownSignalException cause) {
//					log("shutdownCompleted");
//					try {
//						close();
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//				}
//			});

			if (mainChannel!=null) {
				mainChannel.queueDeclare(queueName, durable, exclusive, autoDelete, null);
			}
			return mainChannel;
		} 
		catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public void close()  {

		log("close");
		Channel ch = mainChannel;
		Connection con = connection;
		
		mainChannel = null;
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
