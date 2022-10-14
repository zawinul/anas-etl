package it.eng.anas.model;

import it.eng.anas.Utils;

public class Esempio1 extends Model {
	public String id = Utils.rndString(6);
	public int code = 1000+(int)(Math.random()*9000);
	public String campo1 = Utils.rndString(2);
	public Child1 child1 = new Child1();
	public int nretry = 0;
	
	public static class Child1 extends Model {
		public int response = 42;
	}
	
}
