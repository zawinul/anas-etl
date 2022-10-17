package it.eng.anas;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.filenet.api.collection.IdList;
import com.filenet.api.collection.IndependentObjectSet;
import com.filenet.api.collection.PageIterator;
import com.filenet.api.collection.StringList;
import com.filenet.api.core.Connection;
import com.filenet.api.core.Factory;
import com.filenet.api.core.Folder;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.exception.EngineRuntimeException;
import com.filenet.api.property.Properties;
import com.filenet.api.property.Property;
import com.filenet.api.property.PropertyBoolean;
import com.filenet.api.property.PropertyDateTime;
import com.filenet.api.property.PropertyFloat64;
import com.filenet.api.property.PropertyId;
import com.filenet.api.property.PropertyIdList;
import com.filenet.api.property.PropertyInteger32;
import com.filenet.api.property.PropertyString;
import com.filenet.api.property.PropertyStringList;
import com.filenet.api.query.SearchSQL;
import com.filenet.api.query.SearchScope;
import com.filenet.api.util.UserContext;

import it.eng.anas.model.Config;
import it.eng.anas.model.FnSecurityContext;
import it.eng.anas.model.Model;

public class FilenetHelper {
	private Subject subject=null;
	private Connection connection=null;
	//private ObjectStore os=null;

	private ObjectMapper mapper = Utils.getMapper();
	private static Logger logger = LoggerFactory.getLogger("filenet");

	public void initFilenetAuthentication(FnSecurityContext securityContext){
	    try {
	    	Config c = Utils.getConfig();
	    	connection = Factory.Connection.getConnection(c.filenet.uri);
	    	subject = UserContext.createSubject(connection, 
	    			c.filenet.userid, 
	    			c.filenet.password, 
	    			c.filenet.stanza);
	    } catch (EngineRuntimeException e) {
	    	logger.error("Unable to authenticated at context ");
	    	throw e;
	    }
	 }
	
	
	
	public List<String> getFolderIdByClassName(String classeDocumentale,ObjectStore os) throws Exception {
		String query = "SELECT * FROM "+classeDocumentale;
		os.refresh();
		SearchScope scope = new SearchScope(os);
		SearchSQL sql = new SearchSQL(query);
		int PAGE_SIZE = 100;
		IndependentObjectSet docSet = scope.fetchObjects(sql, PAGE_SIZE,null, true);

		ArrayList<String> ret = new ArrayList<String>();
		PageIterator pageIterator = docSet.pageIterator();
		while (pageIterator.nextPage()) {
			for (Object obj : pageIterator.getCurrentPage()) {
				Folder f = (Folder) obj;
				ret.add(f.get_Id().toString());
			}
		}
		return ret;
	}
	
	public static class FolderTreeNode extends Model {
		public String id;
		public List<FolderTreeNode> child = new ArrayList<FolderTreeNode>();
		public List<String> doc = new ArrayList<String>();
	};
	
	
	public FolderTreeNode getFolderTree(String folderId) { 
		FolderTreeNode ret = new FolderTreeNode();
		ret.id = folderId;
		
		return ret;
	}

	public List<Folder> getFolderByClassName(String classeDocumentale,ObjectStore os) throws Exception {
		String query = "SELECT * FROM "+classeDocumentale;
		os.refresh();
		SearchScope scope = new SearchScope(os);
		SearchSQL sql = new SearchSQL(query);
		int PAGE_SIZE = 100;
		IndependentObjectSet docSet = scope.fetchObjects(sql, PAGE_SIZE,null, true);

		ArrayList<Folder> ret = new ArrayList<Folder>();
		// Get the page iterator
		PageIterator pageIterator = docSet.pageIterator();
		while (pageIterator.nextPage()) {
			// Loop through each item in the page
			for (Object obj : pageIterator.getCurrentPage()) {
				// Get the document object and write Document Title
				Folder f = (Folder) obj;
				Properties props = f.getProperties();
				@SuppressWarnings("rawtypes")
				Iterator iterator = props.iterator();
				while(iterator.hasNext()) {
					Property p = (Property) iterator.next();
					String name = p.getPropertyName();
					
				}
				ret.add(f);
			}
		}
		return ret;
	}

	public JsonNode extractProperties(Properties props) {
		ObjectNode node = mapper.createObjectNode();
		@SuppressWarnings("rawtypes")
		Iterator iterator = props.iterator();
		while(iterator.hasNext()) {
			Property p = (Property) iterator.next();
			String name = p.getPropertyName();
			setJsonPropertyValue(p, name, node);
		}
		return node;
	}
	
	public void setJsonPropertyValue(Property prop, String name, ObjectNode node) {
//		PropertyBinary
//		PropertyBinaryList
//		PropertyBoolean
//		PropertyContent
//		PropertyDateTimeList
//		PropertyDependentObjectList
//		PropertyIndependentObjectSet
//		PropertyInteger32
		
		if (prop instanceof PropertyBoolean) {
			node.put(name, prop.getBooleanValue());
		}
		else if (prop instanceof PropertyString) {
			node.put(name, prop.getStringValue());
		}
		else if (prop instanceof PropertyStringList) {
			StringList list = prop.getStringListValue();
			int size = list.size();
			ArrayNode array = mapper.createArrayNode();
			for(int i=0;i<size;i++) {
				String val = (String) list.get(i);
				array.set(i,  val);
			}
			node.set(name, array);
		}
		else if (prop instanceof PropertyDateTime) {
			node.put(name, prop.getDateTimeValue().toString());
		}
		else if (prop instanceof PropertyInteger32) {
			node.put(name, prop.getInteger32Value());
		}
		else if (prop instanceof PropertyFloat64) {
			node.put(name, prop.getFloat64Value());
		}
		else if (prop instanceof PropertyId) {
			node.put(name, prop.getIdValue().toString());
		}
		else if (prop instanceof PropertyIdList) {
			IdList list = prop.getIdListValue();
			int size = list.size();
			ArrayNode array = mapper.createArrayNode();
			for(int i=0;i<size;i++) {
				String val = (String) list.get(i).toString();
				array.set(i,  val);
			}
			node.set(name, node);
		}
		else {
			node.put(name, prop.getObjectValue().toString()+" (unmanaged type)");
		}
	}
}
