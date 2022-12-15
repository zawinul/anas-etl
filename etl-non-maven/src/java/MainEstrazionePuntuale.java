

import com.fasterxml.jackson.databind.node.ObjectNode;

import it.eng.anas.Event;
import it.eng.anas.FileHelper;
import it.eng.anas.FilenetHelper;

public class MainEstrazionePuntuale {
	//const outdir = "c:\\temp\\buttami\\ep";
	public String campioni[] = new String [] {
			"pdmDBSProgetto,A288EE0E-6735-475C-A935-3DC3ED8E24C1,AE2B728A-BFF5-477A-90A6-864EBBD653F6,B4120E57-D08D-4E5E-AEB9-09E260F71573",
			"pdmElaboratoProgettuale,A40C8AB7-42C2-485B-87B1-5DF250596B31,A413D3E0-4612-4D03-96AD-FC3B9571B503,A418261C-9464-4071-A907-ACEBB1082746",
			"pdmDispositivoEDelibera,A7A59A79-A65E-4887-BB45-B26A9B56717D,A980A4FE-C2FD-474E-9883-6AD5CFB6BACB,A18F30D0-2416-41C0-B8F4-38938546CF49",
			"pdmDatiDiIngresso,A5C4B0F7-8631-4AE3-85B3-017897D40BBA,A73494CB-85B2-41D6-BC08-97B642E4432D,A879319A-9A5C-4ED1-82D3-3F251599BE41",
			"pdmElencoElaboratiDiProgetto,A5C329BD-BE8E-4F81-8E23-99937D2D222E,A0E8807A-0000-C418-A303-73194437DCD3,AA259305-6E40-4D26-869C-A105331903BF"			
	};
	
	public void elabora() throws Exception {
		FilenetHelper filenet = new FilenetHelper();
		filenet.initFilenetAuthentication();
		FileHelper fhelp = new FileHelper();
		for(String row: campioni) {
			String cells[] = row.split(",");
			String classe = cells[0];
			for(int i=1; i<cells.length;i++) {
				try {
					String filename = "estrazioni/"+classe+"."+cells[i]+".json";
					System.out.println(filename	);
					ObjectNode node = filenet.getDocumentMetadata("PDM", "{"+cells[i]+"}");
					fhelp.saveJsonObject(filename, node);
				} catch (Exception e) {
					// e.printStackTrace();
					System.out.println(e.getMessage());
				}
				
			}
		}
		
	}
	
	
	public static void main(String args[]) throws Exception{
		new MainEstrazionePuntuale().elabora();
		Event.emit("exit");
		System.out.println("done!");

	}
	

}
