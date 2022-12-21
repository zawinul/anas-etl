package it.eng.anas.model;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Log;
import it.eng.anas.Utils;

public abstract class Model {
	
	private static ObjectMapper mapper = Utils.getMapper();
	public String toString() {
		try {
			return mapper.writeValueAsString(this);
		} catch (Exception e) {
			Log.log("model to string error: "+e.getMessage());
			return super.toString();
		}
	}
}
