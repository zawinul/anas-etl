package it.eng.anas.main;

import java.io.FileReader;
import java.io.FileWriter;

import it.eng.anas.Log;
import it.eng.anas.Utils;
import it.eng.anas.model.Config;
import it.eng.anas.model.Model;

public class MainTestConfig extends Model {
	public static void main(String args[]) throws Exception {
		Config c = new Config();
		c.schedule.put("lun 07:10", 5);
		c.schedule.put("mer 18:40", 8);
		FileWriter w = new FileWriter("./config-sample.json");
		w.write(Utils.getMapper().writeValueAsString(c));
		w.close();
		
		Config c2 = Utils.getMapper().readValue(new FileReader("./config-sample.json"), Config.class);
		String c2json = Utils.getMapper().writeValueAsString(c2);
		Log.main.log(c2json);
		
//		long t = new Date().getTime();
//		SimpleDateFormat dateFormat = new SimpleDateFormat("EEE HH:mm", Locale.ITALIAN);
//		for(int i=0; i<24; i++) {
//			Date d = new Date(t+i*60*60*1000);
//			String formatted = dateFormat.format(d);
//			Log.main.log(formatted);
//		}
//		
//		Log.main.log("ok");
////		GregorianCalendar g =  new GregorianCalendar();
////		g.setTimeInMillis(new Date().getTime()+24*60*60*1000);
////		int day = g.get(Calendar.DAY_OF_WEEK);
////		System.out.println(day);
	}
}
