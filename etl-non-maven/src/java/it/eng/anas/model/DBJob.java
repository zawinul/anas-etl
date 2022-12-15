package it.eng.anas.model;




public class DBJob extends Model {	
	public int id;
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

	public int parent_job;
	public int duration;
	public String dir;
	
	public String output;
	

	public DBJob() {}
//	public DBJob(int id, String locktag, Integer priority, Integer nretry, String queue, String command, String key1,
//			String key2, String key3, String creation, String last_change, int parent_job, int duration, String body) {
//		super();
//		this.id = id;
//		this.locktag = locktag;
//		this.priority = priority;
//		this.nretry = nretry;
//		this.queue = queue;
//		this.operation = command;
//		this.key1 = key1;
//		this.key2 = key2;
//		this.key3 = key3;
//		this.creation = creation;
//		this.last_change = last_change;
//		this.parent_job = parent_job;
//		this.duration = duration;
//	}
	
	

}
