package it.eng.anas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JSON  {

	private static ObjectMapper mapper = Utils.getMapper();
	private static ObjectMapper mapperOneLine = Utils.getMapperOneLine();
	
	public static ObjectNode object(Object... values) {
		int n = values.length;
		if ((n%2)==1)
			throw new RuntimeException("obj: # of params must be even");
		
		ObjectNode node = mapper.createObjectNode();
		for(int i=0; i<n; i+=2) {
			String key = (String) values[i];
			Object value = values[i+1];
			node.set(key,  mapper.valueToTree(value));
		}
		return node;
	} 

	
	public static ArrayNode array(Object... values) {
		ArrayNode node = mapper.createArrayNode();
		for(int i=0; i<values.length; i++) {
			node.add(mapper.valueToTree(values[i]));
		}
		return node;
	} 

	public static String string(Object x) {
		try {
			return mapperOneLine.writeValueAsString(x);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static String string(Object x, boolean indent) {
		try {
			return indent ? mapper.writeValueAsString(x) : mapperOneLine.writeValueAsString(x);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return null;
		}
	}
}
