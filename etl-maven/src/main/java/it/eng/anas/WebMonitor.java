package it.eng.anas;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import it.eng.anas.main.MainGetQueues.QDesc;

public class WebMonitor {

	private static String user = "paolo";
	private static String password = "paolo";
	
	private ObjectMapper mapper = Utils.getMapper();
	
	public List<QDesc> getQueues() throws Exception {
		// curl -i --header "authorization: Basic cGFvbG86cGFvbG8=" http://localhost:15672/api/vhosts
		//String auth = "cGFvbG86cGFvbG8=";
		URL url = new URL("http://localhost:15672/api/queues");
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		
		String auth = user + ":" + password;
		byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
		String authHeaderValue = "Basic " + new String(encodedAuth);
		con.setRequestProperty("Authorization", authHeaderValue);
		
		con.setRequestProperty("Content-Type", "application/json");
		con.setRequestProperty("Accept", "application/json");

		con.setRequestMethod("GET");

		int status = con.getResponseCode();
		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer content = new StringBuffer();
		while ((inputLine = in.readLine()) != null) 
		    content.append(inputLine);
		in.close();
		
		String json = content.toString();
		
		List<QDesc> ret = new ArrayList<QDesc>();
		ArrayNode array = (ArrayNode) mapper.readTree(json);
		for(int i=0; i<array.size(); i++) {
			QDesc desc = mapper.treeToValue(array.get(i),  QDesc.class);
			ret.add(desc);
		}

		return ret;
	}
}
