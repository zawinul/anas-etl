package it.eng.anas.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Utils;

public class DBJobT extends DBJob {	
	private ObjectMapper mapper;
	public Esempio1 getEsempio1() {
		try {
			if (mapper==null)
				mapper = Utils.getMapperOneLine();
			return mapper.readValue(body, Esempio1.class);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	public void setEsempio1(Esempio1 obj) {
		try {
			if (mapper==null)
				mapper = Utils.getMapperOneLine();
			body = mapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
	
}
