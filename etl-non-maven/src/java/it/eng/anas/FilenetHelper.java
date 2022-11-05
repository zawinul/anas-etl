package it.eng.anas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.filenet.api.collection.ContentElementList;
import com.filenet.api.collection.DateTimeList;
import com.filenet.api.collection.DependentObjectList;
import com.filenet.api.collection.DocumentSet;
import com.filenet.api.collection.IdList;
import com.filenet.api.collection.IndependentObjectSet;
import com.filenet.api.collection.StringList;
import com.filenet.api.constants.FilteredPropertyType;
import com.filenet.api.constants.PropertyNames;
import com.filenet.api.core.Connection;
import com.filenet.api.core.ContentTransfer;
import com.filenet.api.core.Document;
import com.filenet.api.core.Domain;
import com.filenet.api.core.Factory;
import com.filenet.api.core.Folder;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.property.FilterElement;
import com.filenet.api.property.Properties;
import com.filenet.api.property.Property;
import com.filenet.api.property.PropertyBoolean;
import com.filenet.api.property.PropertyDateTime;
import com.filenet.api.property.PropertyDateTimeList;
import com.filenet.api.property.PropertyDependentObjectList;
import com.filenet.api.property.PropertyFilter;
import com.filenet.api.property.PropertyFloat64;
import com.filenet.api.property.PropertyId;
import com.filenet.api.property.PropertyIdList;
import com.filenet.api.property.PropertyIndependentObjectSet;
import com.filenet.api.property.PropertyInteger32;
import com.filenet.api.property.PropertyString;
import com.filenet.api.property.PropertyStringList;
import com.filenet.api.util.Id;
import com.filenet.api.util.UserContext;
import com.filenet.apiimpl.core.FolderImpl;

import it.eng.anas.model.Config;

public class FilenetHelper {
	protected Subject subject=null;
	protected Connection connection=null;
	protected Domain domain;
	
	public Map<String,ObjectStore> osmap=new HashMap<String, ObjectStore>();

	protected ObjectMapper mapper = Utils.getMapper();
	private boolean authInitialized = false;
	
	protected static Logger logger = LoggerFactory.getLogger("filenet");

	protected static String commonPropertiesExclusion[] = {
		"FoldersFiledIn",
		"Containers",
		"FoldersFiledIn",
		"Permissions",
		"Parent",
		"Annotations",
		"ClassDescription",
		"This",
		"WorkflowSubscriptions"
	};
	protected static String folderPropertiesExclusion[] = {};
	protected static String documentPropertiesExclusion[] = {};
	
	public void initFilenetAuthentication() throws Exception{
		if (authInitialized)
			return;
		
		Config c = Utils.getConfig();
    	connection = Factory.Connection.getConnection(c.filenet.uri);
    	
    	subject = UserContext.createSubject(connection, 
    			c.filenet.userid, 
    			c.filenet.password, 
    			c.filenet.stanza);
    	UserContext uc = UserContext.get();
    	uc.pushSubject(subject);
    	domain = Factory.Domain.fetchInstance(connection,null, null);
    	
    	authInitialized = true;
	 }
	
	public ObjectStore getOS(String osName) throws Exception {
		initFilenetAuthentication();
		if (osmap.containsKey(osName)) 
			return osmap.get(osName);
			
		ObjectStore os = Factory.ObjectStore.fetchInstance(domain, osName,null);
		if (os!=null)
			osmap.put(osName,  os);
		
		return os;
	}


	public ObjectNode  getFolderMetadataByPath(String objectStore, String path, boolean withDocs) throws Exception {
		ObjectStore os = getOS(objectStore);
		if (os==null)
			throw new Exception("Non esiste l'ObjectStore "+os);


		PropertyFilter pf = new PropertyFilter();
		pf.setMaxRecursion(0);

		pf.addIncludeType(new FilterElement(0, null, null, FilteredPropertyType.ANY, null));
		pf.addIncludeProperty(new FilterElement(2, null, null, PropertyNames.ID, null));
		pf.addExcludeProperty(PropertyNames.SUB_FOLDERS);
		if (!withDocs) {
			pf.addExcludeProperty(PropertyNames.CONTAINED_DOCUMENTS);
			pf.addExcludeProperty(PropertyNames.CONTAINEES);
		}
		
		for(String p:commonPropertiesExclusion)
			pf.addExcludeProperty(p);
		for(String p:folderPropertiesExclusion)
			pf.addExcludeProperty(p);
			
		Folder f = Factory.Folder.fetchInstance(os, path, pf);
		
		FolderImpl fi = (FolderImpl) f;
		ObjectNode ret = mapper.createObjectNode();
		for(Property ap: fi.getPropertiesImpl().toArray()) 
			setJsonPropertyValue(ap, ap.getPropertyName(), ret);
		
		if (withDocs) {
			DocumentSet ds = f.get_ContainedDocuments();
			@SuppressWarnings("unchecked")
			List<Document> docs = getList(ds.iterator());
			List<String> docIds = new ArrayList<String>();
			for(Document doc: docs) 
				docIds.add(doc.get_Id().toString());				
			
			ret.set("__documents__", mapper.valueToTree(docIds));
		}
		return ret;
	}


	public ObjectNode  getFolderMetadataById(String objectStore, String id, boolean withDocs) throws Exception {
		ObjectStore os = getOS(objectStore);
		if (os==null)
			throw new Exception("Non esiste l'ObjectStore "+os);


		PropertyFilter pf = new PropertyFilter();
		pf.setMaxRecursion(0);

		pf.addIncludeType(new FilterElement(0, null, null, FilteredPropertyType.ANY, null));
		pf.addIncludeProperty(new FilterElement(2, null, null, PropertyNames.ID, null));
		pf.addExcludeProperty(PropertyNames.SUB_FOLDERS);
		if (!withDocs) {
			pf.addExcludeProperty(PropertyNames.CONTAINED_DOCUMENTS);
			pf.addExcludeProperty(PropertyNames.CONTAINEES);
		}
		
		for(String p:commonPropertiesExclusion)
			pf.addExcludeProperty(p);
		for(String p:folderPropertiesExclusion)
			pf.addExcludeProperty(p);
			
		//Folder f = Factory.Folder.fetchInstance(os, path, pf);
		Folder f = Factory.Folder.fetchInstance(os, new Id(id), pf);
		
		FolderImpl fi = (FolderImpl) f;
		ObjectNode ret = mapper.createObjectNode();
		for(Property ap: fi.getPropertiesImpl().toArray()) 
			setJsonPropertyValue(ap, ap.getPropertyName(), ret);
		
		if (withDocs) {
			DocumentSet ds = f.get_ContainedDocuments();
			@SuppressWarnings("unchecked")
			List<Document> docs = getList(ds.iterator());
			List<String> docIds = new ArrayList<String>();
			for(Document doc: docs) 
				docIds.add(doc.get_Id().toString());				
			
			ret.set("__documents__", mapper.valueToTree(docIds));
		}
		return ret;
	}

	public ObjectNode  getDocumentMetadata(String objectStore, String id) throws Exception {
		ObjectStore os = getOS(objectStore);
		if (os==null)
			throw new Exception("Non esiste l'ObjectStore "+os);

		PropertyFilter pf = new PropertyFilter();
		pf.setMaxRecursion(0);
		for(String p:commonPropertiesExclusion)
			pf.addExcludeProperty(p);
		for(String p:documentPropertiesExclusion)
			pf.addExcludeProperty(p);

		Document doc = Factory.Document.fetchInstance(os, new Id(id), pf);
		Properties props = doc.getProperties();
		
		@SuppressWarnings("unchecked")
		Iterator<Property> iter1 = props.iterator();
		ObjectNode ret = mapper.createObjectNode();
		while(iter1.hasNext()) {
			Property prop = iter1.next();
			setJsonPropertyValue(prop, prop.getPropertyName(), ret);
		}

		ContentElementList clist = doc.get_ContentElements();
		@SuppressWarnings("unchecked")
		Iterator<ContentTransfer> cIterator = clist.iterator(); 
		List<ContentTransfer> contents = getList(cIterator);
		ObjectMapper mapper = Utils.getMapper();
		ArrayNode contentArray = mapper.createArrayNode();
		for(ContentTransfer ct: contents) {
			HashMap<String, String> map = new HashMap<String, String>();
			map.put("type",ct.get_ContentType());
			map.put("retrievalName",ct.get_RetrievalName());
			Connection conn = ct.getConnection();
			map.put("connectionType",conn.getConnectionType().toString());
			map.put("uri",conn.getURI());
			contentArray.add(mapper.valueToTree(map));
		} 
		ret.set("mappedContent", contentArray);

		
		return ret;
	}


	public List<String>  getDocumentsId(String objectStore, String path) throws Exception {
		ObjectStore os = getOS(objectStore);
		if (os==null)
			throw new Exception("Non esiste l'ObjectStore "+os);

		PropertyFilter pf = new PropertyFilter();
		pf.addIncludeProperty(new FilterElement(1, null, null, PropertyNames.ID, null));
		pf.addIncludeProperty(new FilterElement(0, null, null, PropertyNames.CONTAINED_DOCUMENTS, null));
		Folder f = Factory.Folder.fetchInstance(os, path, pf);
		DocumentSet s = f.get_ContainedDocuments();
		List<String> docIds = new ArrayList<String>();
		@SuppressWarnings("unchecked")
		Iterator<Document> dociterator = s.iterator();
		while(dociterator.hasNext()) {
			Document doc = dociterator.next();
			docIds.add(doc.get_Id().toString());
		}
		return docIds;
	}


	public ArrayNode  getDocumentContent(String objectStore, String docId) throws Exception {
		ObjectStore os = getOS(objectStore);
		if (os==null)
			throw new Exception("Non esiste l'ObjectStore "+os);

		PropertyFilter pf = new PropertyFilter();
		pf.addIncludeProperty(new FilterElement(1, null, null, PropertyNames.ID, null));
		pf.addIncludeProperty(new FilterElement(1, null, null, PropertyNames.CONTAINED_DOCUMENTS, null));
		Document doc = Factory.Document.fetchInstance(os, new Id(docId), pf);
		ContentElementList clist = doc.get_ContentElements();
		@SuppressWarnings("unchecked")
		Iterator<ContentTransfer> cIterator = clist.iterator(); 
		List<ContentTransfer> contents = getList(cIterator);
		ObjectMapper mapper = Utils.getMapper();
		ArrayNode ret = mapper.createArrayNode();
		for(ContentTransfer ct: contents) {
			HashMap<String, String> map = new HashMap<String, String>();
			map.put("type",ct.get_ContentType());
			map.put("retrievalName",ct.get_RetrievalName());
			Connection conn = ct.getConnection();
			map.put("connectionType",conn.getConnectionType().toString());
			map.put("uri",conn.getURI());
			ret.add(mapper.valueToTree(map));
		} 
		return ret;
		
	}

	public List<ContentTransfer>  getContentTransfer(String objectStore, String docId) throws Exception {
		ObjectStore os = getOS(objectStore);
		if (os==null)
			throw new Exception("Non esiste l'ObjectStore "+os);

		PropertyFilter pf = new PropertyFilter();
		pf.addIncludeProperty(new FilterElement(1, null, null, PropertyNames.ID, null));
		pf.addIncludeProperty(new FilterElement(1, null, null, PropertyNames.CONTENT_ELEMENTS, null));
		pf.addIncludeProperty(new FilterElement(1, null, null, PropertyNames.RETRIEVAL_NAME, null));
		Document doc = Factory.Document.fetchInstance(os, new Id(docId), pf);
		ContentElementList clist = doc.get_ContentElements();
		@SuppressWarnings("unchecked")
		Iterator<ContentTransfer> cIterator = clist.iterator(); 
		List<ContentTransfer> contents = getList(cIterator);
		return contents;
		
	}

	private <T> List<T> getList(Iterator<T> iterator) {
		List<T> ret = new ArrayList<>();
		while(iterator.hasNext()) {
			T t = iterator.next();
			ret.add(t);
		}
		return ret;
	}


	public List<String>  getSubfoldersPaths(String objectStore, String path) throws Exception {
		ObjectStore os = getOS(objectStore);
		if (os==null)
			throw new Exception("Non esiste l'ObjectStore "+os);

		PropertyFilter pf = new PropertyFilter();
		pf.addIncludeProperty(new FilterElement(1, null, null, PropertyNames.PATH_NAME, null));
		pf.addIncludeProperty(new FilterElement(1, null, null, PropertyNames.SUB_FOLDERS, null));
		Folder f = Factory.Folder.fetchInstance(os, path, pf);
		@SuppressWarnings("unchecked")
		List<Folder> sublist = getList(f.get_SubFolders().iterator());
		List<String> ret = new ArrayList<String>();
		for(Folder sub: sublist)
			ret.add(sub.get_PathName());
		return ret;
	}


	public List<String>  getSubfoldersId(String objectStore, String path) throws Exception {
		ObjectStore os = getOS(objectStore);
		if (os==null)
			throw new Exception("Non esiste l'ObjectStore "+os);

		PropertyFilter pf = new PropertyFilter();
		pf.addIncludeProperty(new FilterElement(1, null, null, PropertyNames.ID, null));
		pf.addIncludeProperty(new FilterElement(1, null, null, PropertyNames.SUB_FOLDERS, null));
		Folder f = Factory.Folder.fetchInstance(os, path, pf);
		@SuppressWarnings("unchecked")
		List<Folder> sublist = getList(f.get_SubFolders().iterator());
		List<String> ret = new ArrayList<String>();
		for(Folder sub: sublist)
			ret.add(sub.get_Id().toString());
		return ret;
	}

//	public List<String>  getRecursiveFolders(String objectStore, String path) throws Exception {
//		ObjectStore os = getOS(objectStore);
//		if (os==null)
//			throw new Exception("Non esiste l'ObjectStore "+os);
//
//		PropertyFilter pf = new PropertyFilter();
//		pf.addIncludeProperty(new FilterElement(30, null, null, PropertyNames.PATH_NAME, null));
//		pf.addIncludeProperty(new FilterElement(30, null, null, PropertyNames.SUB_FOLDERS, null));
//		Folder f = Factory.Folder.fetchInstance(os, path, pf);
//		List<Folder> todo = new ArrayList<Folder>();
//		List<Folder> done = new ArrayList<Folder>();
//		todo.add(f);
//		
//		while(!todo.isEmpty()) {
//			Folder cur = todo.get(0);
//			Log.fnet.log("tree: "+cur.get_PathName());
//			todo.remove(cur);
//			
//			if (done.contains(cur))
//				continue;
//			
//			done.add(cur);
//			@SuppressWarnings("unchecked")
//			List<Folder> sublist = getList(cur.get_SubFolders().iterator());
//			for(Folder sub:sublist)
//				done.add(sub);
//		}
//		List<String> ret = new ArrayList<String>();
//		for(Folder f2: done)
//			ret.add(f2.get_PathName());
//		return ret;
//	}

//	public List<String> getFolderIdByClassName(String classeDocumentale,ObjectStore os) throws Exception {
//		String query = "SELECT * FROM "+classeDocumentale;
//		os.refresh();
//		SearchScope scope = new SearchScope(os);
//		SearchSQL sql = new SearchSQL(query);
//		int PAGE_SIZE = 100;
//		IndependentObjectSet docSet = scope.fetchObjects(sql, PAGE_SIZE,null, true);
//
//		ArrayList<String> ret = new ArrayList<String>();
//		PageIterator pageIterator = docSet.pageIterator();
//		while (pageIterator.nextPage()) {
//			for (Object obj : pageIterator.getCurrentPage()) {
//				Folder f = (Folder) obj;
//				ret.add(f.get_Id().toString());
//			}
//		}
//		return ret;
//	}
	

//	public List<Folder> getFolderByClassName(String classeDocumentale,ObjectStore os) throws Exception {
//		String query = "SELECT * FROM "+classeDocumentale;
//		os.refresh();
//		SearchScope scope = new SearchScope(os);
//		SearchSQL sql = new SearchSQL(query);
//		int PAGE_SIZE = 100;
//		// Specify a property filter to use for the filter parameter, if needed.
//		// This can be null if you are not filtering properties.
//		PropertyFilter myFilter = new PropertyFilter();
//		int myFilterLevel = 1;
//		myFilter.setMaxRecursion(myFilterLevel);		
//		myFilter.addIncludeType(new FilterElement(null, null, null, FilteredPropertyType.ANY, null));
//
//		IndependentObjectSet docSet = scope.fetchObjects(sql, PAGE_SIZE, myFilter, true);
//
//		ArrayList<Folder> ret = new ArrayList<Folder>();
//		// Get the page iterator
//		PageIterator pageIterator = docSet.pageIterator();
//		while (pageIterator.nextPage()) {
//			// Loop through each item in the page
//			for (Object obj : pageIterator.getCurrentPage()) {
//				// Get the document object and write Document Title
//				Folder f = (Folder) obj;
//				Properties props = f.getProperties();
//				Iterator iterator = props.iterator();
//				while(iterator.hasNext()) {
//					//Property p = (Property) iterator.next();
//					//String name = p.getPropertyName();
//					
//				}
//				ret.add(f);
//			}
//		}
//		return ret;
//	}

	public JsonNode extractProperties(Properties props) {
		ObjectNode node = mapper.createObjectNode();
		Iterator iterator = props.iterator();
		while(iterator.hasNext()) {
			Property p = (Property) iterator.next();
			String name = p.getPropertyName();
			if (p.getObjectValue()==null)
				continue;
			setJsonPropertyValue(p, name, node);
		}
		return node;
	}
	
	public void setJsonPropertyValue(Property prop, String name, ObjectNode node) {
//		PropertyBinary
//		PropertyBinaryList
//		PropertyContent
//		PropertyIndependentObjectSet
		
		if (prop instanceof PropertyBoolean) {
			node.put(name, prop.getBooleanValue());
		}
		else if (prop instanceof PropertyString) {
			node.put(name, prop.getStringValue());
		}
		else if (prop instanceof PropertyStringList) {
			StringList list = prop.getStringListValue();
			int size = list.size();
			List<String> array = new ArrayList<String>();
			for(int i=0;i<size;i++) {
				String val = (String) list.get(i);
				array.add(val);
			}
			node.set(name, mapper.valueToTree(array));
		}
		else if (prop instanceof PropertyDateTime) {
			node.put(name, toString(prop.getDateTimeValue()));
		}
		else if (prop instanceof PropertyDateTimeList) {
			DateTimeList list = prop.getDateTimeListValue();
			int size = list.size();
			List<String> array = new ArrayList<String>();
			for(int i=0;i<size;i++) {
				String val = (String) list.get(i);
				array.add(val);
			}
			node.set(name, mapper.valueToTree(array));
		}
		else if (prop instanceof PropertyInteger32) {
			node.put(name, prop.getInteger32Value());
		}
		else if (prop instanceof PropertyFloat64) {
			node.put(name, prop.getFloat64Value());
		}
		else if (prop instanceof PropertyId) {
			node.put(name, toString(prop.getIdValue()));
		}
		else if (prop instanceof PropertyIdList) {
			IdList list = prop.getIdListValue();
			int size = list.size();
			List<String> array = new ArrayList<String>();
			for(int i=0;i<size;i++) {
				String val = (String) list.get(i);
				array.add(val);
			}
			node.set(name, mapper.valueToTree(array));
		}
		else if (prop instanceof PropertyDependentObjectList) {
			DependentObjectList list = prop.getDependentObjectListValue();
			int size = list.size();
			List<String> array = new ArrayList<String>();
			for(int i=0;i<size;i++) {
				String val = (String) list.get(i).toString();
				array.add(val);
			}
			node.set(name, mapper.valueToTree(array));
		}
		else if (prop instanceof PropertyIndependentObjectSet) {
			IndependentObjectSet set = prop.getIndependentObjectSetValue();
			List<String> array = new ArrayList<String>();
			Iterator iter = set.iterator();
			while(iter.hasNext()) {
				String val = toString(iter.next());
				array.add(val);
			}
			node.set(name, mapper.valueToTree(array));
		}
		
		else {
			Object v = prop.getObjectValue();
			if (v!=null) {
				node.put(name, "UT: "+toString(prop.getObjectValue())+" (unmanaged type "+prop.getClass().getName()+")");
			}
		}
	}
	
	private String toString(Object obj) {
		return obj==null ?null :obj.toString();
	}
}
