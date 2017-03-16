package FanJijie;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;
import java.io.*;
public class Configure {
	private Properties _p;
	public Configure(String cfg_file){
		
		InputStream inputStream=null;
		try {
			inputStream = new java.io.FileInputStream(cfg_file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println(cfg_file+"  File Not Found!");
			e.printStackTrace();
		}
//				this.getClass().getClassLoader().getResourceAsStream(System.getProperty("user.dir")+'/'+cfg_file);
		if(inputStream==null)
		{
			System.out.println("inputStream is null");
		}
		_p = new Properties();
		try {
			_p.load(inputStream);
		} catch (IOException ex) {
			ex.printStackTrace();
		}

	}
	public String getCfg(String cfg_key){
		
		return _p.getProperty(cfg_key);
	}
	public Enumeration<Object> keys(){
		
		return _p.keys();
	}
}
