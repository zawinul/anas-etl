package it.eng.anas;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class Utils {
	public static String rndString(int len) {
		char l[]=new char[len];
		for(int i=0; i<len;i++) 
			l[i] = (char) ('A'+Math.random()*25);
		return new String(l);
	}
	
	public static void sleep(int ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static ObjectMapper getMapper() {
		return getMapper(true);
	}
	
	public static ObjectMapper getMapper(boolean indent) {
		ObjectMapper mapper = new ObjectMapper()
			.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
			.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
			.configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, false)
			.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false)
			.configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false)
			.configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, false)
			.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false)
			.configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, false)
			.configure(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY, false)
			.configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, false)
			.configure(DeserializationFeature.FAIL_ON_UNRESOLVED_OBJECT_IDS, false)
		;
		if (indent)
			mapper.enable(SerializationFeature.INDENT_OUTPUT);
			
		return mapper;
	}
	



}
