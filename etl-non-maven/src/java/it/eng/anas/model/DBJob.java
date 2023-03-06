package it.eng.anas.model;


public class DBJob extends Model {	
	public String id;
	public Integer priority;
	public Integer nretry;
	public String queue;
	public String locktag;
	
	public String operation;
	public String key1;
	public String key2;
	public String key3;
	
	public String creation;
	public String last_change;

	public String parent_job;
	public int duration;
	public String dir;
	

	public DBJob() {}
}
