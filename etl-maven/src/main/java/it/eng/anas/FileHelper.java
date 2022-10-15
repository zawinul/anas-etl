package it.eng.anas;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;

import com.fasterxml.jackson.databind.ObjectMapper;

public class FileHelper {

	private ObjectMapper mapper = Utils.getMapper();
	
	public FileHelper() {
		
	}
	public boolean save(String path, String content) throws Exception {
		try {
			File file = getOutputFile(path);
			FileWriter w = new FileWriter(file);
			w.write(content);
			w.close();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public boolean saveJsonObject(String path, Object content) {
		if (content==null)
			return false;
		
		try {
			String json = mapper.writeValueAsString(content);
			File file = getOutputFile(path);
			FileWriter w = new FileWriter(file);
			w.write(json);
			w.close();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	
	public boolean save(String path, InputStream data) {
		if (data==null)
			return false;
		try {
			File file = getOutputFile(path);
			if (file==null)
				return false;
			BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));
			final int BUFSIZE = 16*1024;
			byte buffer[] = new byte[BUFSIZE];

			while(true) {
				int n = data.read(buffer, 0, BUFSIZE);
				if (n<=0)
					break;
				out.write(buffer, 0, n);
			}
			out.close();
			data.close();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	
	public File getOutputDirFile(String filePath)  {
		try {
			String curDirPath = Utils.getConfig().outputBasePath;
			File curDir = new File(curDirPath);

			if (!curDir.exists()) 
				curDir.mkdir();
			
			String parts[] = filePath.split("/");
			for(int i=0; i<parts.length-1; i++) {
				File child = new File(curDir, parts[i]);
				if (!child.exists())
					child.mkdir();
				curDir = child;
			}
			
			return curDir;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	
	public File getOutputFile(String filePath) {
		File dir = getOutputDirFile(filePath);
		if (dir==null)
			return null;
		String parts[] = filePath.split("/");
		File outfile = new File(dir, parts[parts.length-1]);
		return outfile;
	}


}
