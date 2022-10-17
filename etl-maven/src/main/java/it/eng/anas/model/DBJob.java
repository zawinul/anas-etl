package it.eng.anas.model;




public class DBJob extends Model {	
	public int id;
	public Integer priority;
	public Integer nretry;
	public Status status;
	public String queue;

	public String operation;
	public String par1;
	public String par2;
	public String par3;
	
	public String creation;
	public String last_change;
	public String extra;
	
	public static enum Status {
		ready,
		process,
		done,
		error
	}

	public DBJob() {}
	public DBJob(int id, Status status, Integer priority, Integer nretry, String queue, String command, String par1,
			String par2, String par3, String creation, String last_change, String extra) {
		super();
		this.id = id;
		this.status = status;
		this.priority = priority;
		this.nretry = nretry;
		this.queue = queue;
		this.operation = command;
		this.par1 = par1;
		this.par2 = par2;
		this.par3 = par3;
		this.creation = creation;
		this.last_change = last_change;
		this.extra = extra;
	}


}
