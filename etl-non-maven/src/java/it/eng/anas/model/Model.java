package it.eng.anas.model;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Log;
import it.eng.anas.Utils;

public abstract class Model {
	
	private static ObjectMapper oneLineMapper = Utils.getMapperOneLine();
	public String toString() {
		try {
			return oneLineMapper.writeValueAsString(this);
		} catch (Exception e) {
			Log.etl.log("model to string error: "+e.getMessage());
			return super.toString();
		}
	}
}
