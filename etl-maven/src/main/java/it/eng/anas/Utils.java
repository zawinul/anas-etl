package it.eng.anas;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
	
	public static void randomSleep(int msMin, int msMax) {
		sleep(msMin+(int)(Math.random()*(msMax-msMin)));
	}
	
	public static ObjectMapper getMapper() {
		ObjectMapper mapper = getMapperOneLine();
		mapper.enable(SerializationFeature.INDENT_OUTPUT);
		return mapper;
	}

	public static ObjectMapper getMapperOneLine() {
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
		return mapper;
	}
	
	
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssS");
	private static String datePattern = dateFormat.format(new Date(0L));
	private static int dpLen = datePattern.length();

	public static String date2String(Date d) {
		if (d==null)
			return "null";
		else
			return dateFormat.format(d);
	}

	public static Date string2Date(String s) {
		if (s==null)
			return null;
		int len = s.length();
		if (len > dpLen)
			s = s.substring(0, dpLen);
		else if (len < dpLen)
			s = s+datePattern.substring(len);
		try {
			return dateFormat.parse(s);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void main(String args[]) {
		System.out.println(date2String(new Date()).length());
	}
}
