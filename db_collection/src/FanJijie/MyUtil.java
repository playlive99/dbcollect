package FanJijie;
import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;


/**
* 公用工具类
* @author Fanjijie
*  
*/
public class MyUtil {
	static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	//输出LOG
	static synchronized  void log(String msg){
		System.out.println(df.format(new Date())+":"+msg);
	}
}
