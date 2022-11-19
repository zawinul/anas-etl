package it.eng.anas;

import java.util.HashMap;

public class Global {
	public static HashMap<String, Object> map = new HashMap<String, Object>();
	
	public static void set(String key, Object value) {
		map.put(key, value);
	}

	
	public static Object get(String key) {
		return map.get(key);
	}
	
	public static String getString(String key) {
		Object obj = map.get(key);
		return obj==null ? null : obj.toString();
	}

}
