package it.eng.anas;

public class FilenetTest extends FilenetHelper {
//http://10.21.177.54:9080/acce/
// http://p8programmer.blogspot.com/2017/05/sample-java-code-to-set-folder.html
	
	public void test1() throws Exception{
		initFilenetAuthentication();
		System.out.println(""+os);
	}
	
	public static void main(String args[])throws Exception {
		new FilenetTest().test1();
		
		System.out.println("done!");
	}
}
