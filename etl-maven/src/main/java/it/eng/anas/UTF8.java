package it.eng.anas;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;

public class UTF8 {
	public static final  String name = "UTF-8";
	public static final Charset charset= Charset.forName(name);
	
	
	public static String string(byte[] b) {
		try {
			return new String(b, charset);
		} 
		catch (Exception e) {
			return null;
		}
	}
	
	public static byte[] bytes(String b) {
		try {
			return b.getBytes(charset);
		} catch (Exception e) {
			return null;
		}
	}
	
	public static String urlEncode(String x) {
		try {
			return URLEncoder.encode(x, charset);
		} catch (Exception e) {
			return null;
		}
	}

	public static String streamToString(InputStream is) {
		try {
			return IOUtils.toString(is, charset);
		} 
		catch (IOException e) {
			return "Error: "+e.getMessage();
		} 
	}
	
//	// DIGEST
//	public static String md5(String text) {
//		return md5(bytes(text));
//	}
//	
//	public static String md5(byte bytes[]) {
//		return org.apache.commons.codec.digest.DigestUtils.md5Hex(bytes);
//	}
//	
//	public static byte[] md5Bytes(String x) {
//		return md5Bytes(bytes(x));
//	}
//
//	public static byte[] md5Bytes(byte bytes[]) {
//		return org.apache.commons.codec.digest.DigestUtils.md5(bytes);
//	}
//	
//	public static String sha(String text) {
//		return sha(bytes(text));
//	}
//	
//	public static String sha(byte bytes[]) {
//		return org.apache.commons.codec.digest.DigestUtils.shaHex(bytes);
//	}
//	
//	public static byte[] shaBytes(String x) {
//		return shaBytes(bytes(x));
//	}
//
//	public static byte[] shaBytes(byte bytes[]) {
//		return org.apache.commons.codec.digest.DigestUtils.sha(bytes);
//	}

}
