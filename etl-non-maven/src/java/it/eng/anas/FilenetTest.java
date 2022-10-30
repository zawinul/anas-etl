package it.eng.anas;

import java.util.HashMap;
import java.util.Iterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.filenet.api.collection.ClassDescriptionSet;
import com.filenet.api.collection.FolderSet;
import com.filenet.api.collection.IndependentObjectSet;
import com.filenet.api.collection.PageIterator;
import com.filenet.api.collection.PropertyDescriptionList;
import com.filenet.api.collection.ReferentialContainmentRelationshipSet;
import com.filenet.api.constants.PropertyNames;
import com.filenet.api.core.Factory;
import com.filenet.api.core.Folder;
import com.filenet.api.core.ReferentialContainmentRelationship;
import com.filenet.api.meta.ClassDescription;
import com.filenet.api.meta.PropertyDescription;
import com.filenet.api.property.FilterElement;
import com.filenet.api.property.Properties;
import com.filenet.api.property.Property;
import com.filenet.api.property.PropertyFilter;

public class FilenetTest extends FilenetHelper {
// http://10.21.177.54:9080/acce/
// http://p8programmer.blogspot.com/2017/05/sample-java-code-to-set-folder.html
// https://www.notonlyanecmplace.com/understand-the-filenet-property-filters/
	
	public void test1() throws Exception{
		initFilenetAuthentication();
		System.out.println(""+os);
		Folder root = os.get_RootFolder();
		//dumpFolder(root);
		folderTree(root, "");
	}
	
	public void test2() throws Exception {
		initFilenetAuthentication();
		System.out.println(""+os);
		PropertyFilter pf = new PropertyFilter();
		pf.addIncludeProperty(new FilterElement(1, null, null, PropertyNames.SUB_FOLDERS, null));
		pf.addIncludeProperty(new FilterElement(1, null, null, PropertyNames.FOLDER_NAME, null));		
		pf.addIncludeProperty(new FilterElement(1, null, null, "PathName", null));		
		Folder f = Factory.Folder.fetchInstance(os, "/", pf);
		folderTree(f, ""); 
//		FolderSet sfs = f.get_SubFolders();
//		Iterator<Folder> it = sfs.iterator();
//		Folder sf;
//		while (it.hasNext()) {
//		    sf = it.next();
//		    System.out.println(sf.get_PathName());
//		}
		System.out.println("ok");
	}
	
	public void test3() throws Exception {
		initFilenetAuthentication();
		System.out.println(""+os);
		folderTree("/", ""); 
//		FolderSet sfs = f.get_SubFolders();
//		Iterator<Folder> it = sfs.iterator();
//		Folder sf;
//		while (it.hasNext()) {
//		    sf = it.next();
//		    System.out.println(sf.get_PathName());
//		}
		System.out.println("ok");
	}
	
	public void test4() throws Exception {
		initFilenetAuthentication();
		System.out.println(""+os);
		ClassDescriptionSet cds = os.get_ClassDescriptions();
		PageIterator pi = cds.pageIterator();
		HashMap<String,Object> mainlist = new HashMap<String,Object>();
		while(pi.nextPage()) {
			Object page[] = pi.getCurrentPage();
			for(int i=0; i<page.length; i++) {
				ClassDescription cd = (ClassDescription) page[i];
				System.out.println(cd.get_SymbolicName());
				HashMap<String, Object> cdmap = new HashMap<String, Object>();
				mainlist.put(cd.get_SymbolicName(), cdmap);
				cdmap.put("id", cd.get_Id().toString());
				cdmap.put("name", cd.get_Name());
				cdmap.put("description", cd.get_DescriptiveText());
				ClassDescription s = cd.get_SuperclassDescription();
				if (s!=null) {
					cdmap.put("super", s.get_SymbolicName());
					cdmap.put("superId", s.get_Id().toString());
				}
				HashMap<String,Object> props = new HashMap<String,Object>();
				cdmap.put("properties", props);
				PropertyDescriptionList pdl = cd.get_PropertyDescriptions();
				@SuppressWarnings("rawtypes")
				Iterator it2 = pdl.iterator();
				while(it2.hasNext()) {
					PropertyDescription pd = (PropertyDescription) it2.next();
					HashMap<String, Object> prop = new HashMap<String, Object>();
					props.put(pd.get_SymbolicName(), prop);

					prop.put("id", pd.get_Id().toString());
					prop.put("name", pd.get_Name());
					prop.put("description", pd.get_DescriptiveText());
					prop.put("type", pd.get_DataType().toString());
				}				
			}
		}
		new FileHelper().saveJsonObject("descriptors.json", mainlist);
	}

	
	public void test5() throws Exception {
		initFilenetAuthentication();
		System.out.println(""+os);
		Folder root = os.get_RootFolder();
		String rootId = root.get_Id().toString();
		System.out.println(root);

		PropertyFilter pf = new PropertyFilter();
		pf.addIncludeProperty(new FilterElement(null, null, null, PropertyNames.ID, null));
		pf.addIncludeProperty(new FilterElement(null, null, null, PropertyNames.SUB_FOLDERS, null));
		pf.addIncludeProperty(new FilterElement(null, null, null, PropertyNames.FOLDER_NAME, null));		
		pf.addIncludeProperty(new FilterElement(null, null, null, "PathName", null));		

		Folder root2 = Factory.Folder.fetchInstance(os, "/", pf);
		System.out.println(root2);
		//Property sfprop = p.get(PropertyNames.SUB_FOLDERS);
		Properties rp = root2.getProperties();
		IndependentObjectSet list = rp.getIndependentObjectSetValue(PropertyNames.SUB_FOLDERS);
		Iterator it2 = list.iterator();
		while(it2.hasNext()) {
			Folder child = (Folder) it2.next();
			System.out.println("child "+child);
			Properties p = child.getProperties();
			Iterator it = p.iterator();
			System.out.println(child.get_Id().toString());
			while(it.hasNext()) {
				Property prop = (Property) it.next();
				System.out.println("\t"+prop.getPropertyName()+" "+prop.getObjectValue());
			}
		}

	}

	
	public void test6() throws Exception {
		initFilenetAuthentication();
		System.out.println(""+os);
		Folder root = os.get_RootFolder();
		String rootId = root.get_Id().toString();
		System.out.println(root);

		PropertyFilter pf = new PropertyFilter();
		pf.addIncludeProperty(new FilterElement(null, null, null, PropertyNames.ID, null));
		pf.addIncludeProperty(new FilterElement(null, null, null, PropertyNames.SUB_FOLDERS, null));
		pf.addIncludeProperty(new FilterElement(null, null, null, PropertyNames.FOLDER_NAME, null));		
		pf.addIncludeProperty(new FilterElement(null, null, null, "PathName", null));		
		ReferentialContainmentRelationshipSet childs =  root.get_Containees();
		Iterator<ReferentialContainmentRelationship> childIter = childs.iterator();
		while(childIter.hasNext()) {
			ReferentialContainmentRelationship element = childIter.next();
			System.out.println(element.get_Id().toString());
			System.out.println(element.getClass().getName());
		}
	}
	
	public void test7() throws Exception {
		initFilenetAuthentication();
		getDBSList("/");
	}

	
	public void folderTree(String path, String tab) throws Exception {
		PropertyFilter pf = new PropertyFilter();
		pf.addIncludeProperty(new FilterElement(null, null, null, PropertyNames.ID, null));
		pf.addIncludeProperty(new FilterElement(null, null, null, PropertyNames.SUB_FOLDERS, null));
		pf.addIncludeProperty(new FilterElement(null, null, null, PropertyNames.FOLDER_NAME, null));		
		pf.addIncludeProperty(new FilterElement(null, null, null, "PathName", null));		
		Folder f = Factory.Folder.fetchInstance(os, path, pf);
		System.out.println(tab+f.get_Id().toString()+" -  "+f.get_FolderName());
		FolderSet children = f.get_SubFolders();
		@SuppressWarnings("unchecked")
		Iterator<Folder> iter = children.iterator();
		while(iter.hasNext()) {
			Folder sub = iter.next();
			folderTree(sub.get_PathName(), tab+"\t");
		}
	}

	
	public void dumpFolder(Folder f) throws Exception {
		JsonNode j = extractProperties(f.getProperties());
		System.out.println(mapper.writeValueAsString(j));
	}

	public void folderTree(Folder r, String tab) throws Exception {
		System.out.println(tab+r.get_FolderName());
		//System.out.println(tab+"["+r.get_FolderName()+"] (path="+r.get_PathName()+")");
		FolderSet children = r.get_SubFolders();
		Iterator iter = children.iterator();
		while(iter.hasNext()) {
			Folder sub = (Folder)  iter.next();
			folderTree(sub, tab+"\t");
		}
	}
	
	public static void main(String args[])throws Exception {
		new FilenetTest().test7();
		
		Event.emit("exit");
		System.out.println("done!");
		
	}
}
