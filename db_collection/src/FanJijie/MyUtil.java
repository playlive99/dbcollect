package FanJijie;
import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;


/**
* ���ù�����
* @author Fanjijie
*  
*/
public class MyUtil {
	static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	//���LOG
	static synchronized  void log(String msg){
		System.out.println(df.format(new Date())+":"+msg);
	}
}
