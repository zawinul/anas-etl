package it.eng.anas;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.model.Config;

public class ScheduleHelper {

	private static String daysIta[] = {"dom", "lun", "mar", "mer", "gio", "ven", "sab" };
	private static SimpleDateFormat schedDateFormat = new SimpleDateFormat("EEE HH:mm", Locale.ITALIAN);

	private static List<Sched> tab;
	
	public static int getNumOfThread() throws Exception {
		String now = schedDateFormat.format(new Date());
		int min = dateToMin(now);
		int nthread = tab.get(0).nthread;
		for(Sched s: tab) {
			if (s.start>min)
				break;
			nthread = s.nthread;
		}
		return nthread;
	}
	
	private static int dateToMin(String date) throws Exception  {
		int dw = -1;
		String dwstr = date.substring(0,3);
		for(int i=0; i<daysIta.length; i++)
			if (daysIta[i].equals(dwstr))
				dw = i;
		if (dw<0)
			throw new Exception ("nome del giorno della settimana non riconosciuto:"+dwstr);
		
		int hh = Integer.parseInt(date.substring(4,6));
		int mm = Integer.parseInt(date.substring(7,9));
		int ret = dw*24*60+hh*60+mm;
		//Log.main.log(date+"->"+ret+"   hh="+hh+" mm="+mm+" dw="+dw);
		return ret;
	}
	
	private static class Sched {
		public int start;
		public int nthread;

		public Sched(int start, int nthread) {
			super();
			this.start = start;
			this.nthread = nthread;
		}
	}
	
	private static void buildTableFromConfig()  {
		try {
			Config cfg = Utils.getConfig();
			tab = new ArrayList<Sched>();
			
			for(String key: cfg.schedule.keySet()) {
				Integer min = dateToMin(key);
				tab.add(new Sched(min, cfg.schedule.get(key)));
			}
			tab.add(new Sched(0, 0)); // aggiungi inizio settimana
			tab.add(new Sched(7*24*60, 0)); // aggiungi fine settimana
			
			// ordino per time;
			tab.sort(new Comparator<Sched>() {
				@Override
				public int compare(Sched o1, Sched o2) {
					return o1.start-o2.start;
				}
			});
			
			// setto il num thread di inizio e fine settimana
			Sched penultimo = tab.get(tab.size()-2);
			int finalNThread = penultimo.nthread;
			tab.get(0).nthread = finalNThread;
			tab.get(tab.size()-1).nthread = finalNThread;
			
			ObjectMapper m = Utils.getMapperOneLine();
			Log.etl.log("sched_table="+m.writeValueAsString(tab));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	


	static {
		buildTableFromConfig();
		Event.addListener("config-change",new Runnable() {
			public void run() {
				Log.etl.log("rebuild of schedule table due to configuration change");
				buildTableFromConfig();
			}
		});
	}
}
