package it.eng.anas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Consumer;

public class Event {
	
	private static HashMap<String, ArrayList<Runnable>> runnables = new HashMap<String, ArrayList<Runnable>>();
	private static HashMap<String, ArrayList<Consumer<Object>>> consumers = new HashMap<String, ArrayList<Consumer<Object>>>();
	
	public static void addListener(String event, Runnable r) {
		if (!runnables.containsKey(event))
			runnables.put(event, new ArrayList<Runnable>());
		
		runnables.get(event).add(r);
	}
	
	
	public static void addListener(String event, Consumer<Object> r) {
		if (!consumers.containsKey(event))
			consumers.put(event, new ArrayList<Consumer<Object>>());
		
		consumers.get(event).add(r);
	}

	public static void emit(String event) {
		emit(event, null);
	}
	
	public static void emit(String event, Object data) {
		ArrayList<Runnable> listeners = runnables.get(event);
		if (listeners!=null) {
			for(Runnable r: listeners) {
				try {
					r.run();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		ArrayList<Consumer<Object>> clisteners = consumers.get(event);
		if (clisteners!=null) {
			for(Consumer<Object> c: clisteners) {
				try {
					c.accept(data);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}
}
