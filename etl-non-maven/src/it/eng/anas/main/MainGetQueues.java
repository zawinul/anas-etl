package it.eng.anas.main;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

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
		String q = monitor.getQueues();
		System.out.println(q.length()+".");
		ObjectMapper mapper = Utils.getMapper();
		ArrayNode array = (ArrayNode) mapper.readTree(q);
		String json = mapper.writeValueAsString(array);
		System.out.println(json);
		List<QDesc> l = new ArrayList<QDesc>();
		for(int i=0; i<array.size(); i++) {
			JsonNode node = array.get(i);
			QDesc desc = mapper.treeToValue(node,  QDesc.class);
			//System.out.println(desc);
			l.add(desc);
		}
		System.out.println(mapper.writeValueAsString(l));
		//System.out.println(q.substring(0, 100000));
		System.out.println("bye!");
		
	}

}
