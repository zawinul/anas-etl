package it.eng.anas.main;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Utils;
import it.eng.anas.WebMonitor;
import it.eng.anas.model.Model;

public class MainGetQueues {
	public static class QDesc extends Model {
		public String name;
		public String type;
		public String state;
		public int messages;
		public int messages_ready;
		public int messages_unacknowledged;
		public int consumers;
		public boolean auto_delete;
		public String vhost;
	}
	public static void main(String[] args) throws Exception {
		WebMonitor monitor = new WebMonitor();
		List<QDesc> l = monitor.getQueues();
		ObjectMapper mapper = Utils.getMapper();
		System.out.println(mapper.writeValueAsString(l));
		//System.out.println(q.substring(0, 100000));
		System.out.println("bye!");
		
	}

}
