package it.eng.anas.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.eng.anas.Utils;

public abstract class Model {
	
	private static ObjectMapper oneLineMapper = Utils.getMapper(false);
	public String toString() {
		try {
			return oneLineMapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			System.out.println("model to string error: "+e.getMessage());
			return super.toString();
		}
	}
}
