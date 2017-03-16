package FanJijie;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import oracle.sql.BLOB;


public class Data_Collection extends Thread {
	public Data_Collection(String name){
		super(name);

	}

	//static variable
	static ArrayList<DBInfo> src_dbs=new ArrayList<DBInfo>();//源库列表
	static DBInfo target_db=new DBInfo();                    //目标库
	static String source_db_query="";                        //采数SQL
	static String target_table="";                           //目标表及字段列表
	static Connection targetcon=null;                        //目标库Connection
	static ExecutorService threadpool = Executors.newFixedThreadPool(10);  //工作线程池
	static String once="true";                              //只否采集一次后退出,false为按指定时间间隔，重复采集
	static boolean isRun=false;
	static int interval_seconds=60;                       //重复采集时的时间间隔

	public void run() { 
	} 
	
	
	/**
	* 加载配置信息
	* @return
	*  
	*/
	static void  readcfg(String cfgfile)
	{
		MyUtil.log("read configure from "+cfgfile);
		Configure cfg=new Configure(cfgfile);
		Enumeration<Object> cfgkeys = cfg.keys();
		while(cfgkeys.hasMoreElements())
		{
			String cfgkey=(String)cfgkeys.nextElement();
			System.out.println(cfgkey+':'+cfg.getCfg(cfgkey));
			if (cfgkey.toLowerCase().indexOf("source_db_connect_info.")>=0)
			{
				try{
					DBInfo db=new DBInfo();
					String dbinfostr=cfg.getCfg(cfgkey);
					String [] dbinfo_values=dbinfostr.split("#");
					db.db_name=dbinfo_values[0];
					db.ip=dbinfo_values[1];
					db.port=dbinfo_values[2];
					db.service_name=dbinfo_values[3];
					db.username=dbinfo_values[4];
					db.pwd=dbinfo_values[5];
					src_dbs.add(db);
				}
				catch(Exception ex){
					System.out.println("process cfg error:"+cfgkey);
					ex.printStackTrace();
				}
			}
			target_db.db_name="target";
			target_db.ip=cfg.getCfg("target_db_ip");
			target_db.port=cfg.getCfg("target_db_port");
			target_db.service_name=cfg.getCfg("target_service_name");
			target_db.username=cfg.getCfg("target_user");
			target_db.pwd=cfg.getCfg("target_passwd");
			source_db_query=cfg.getCfg("source_db_query");
			target_table=cfg.getCfg("target_table");
			once=cfg.getCfg("once");
			interval_seconds=Integer.valueOf(cfg.getCfg("interval_seconds")); 
		}

	}
	
	
	/**
	* 收集数据主过程(一次运行)
	* @return
	*  
	*/
	static void collectDataOnce(){

		for(int i=0;i<src_dbs.size();i++)
		{ 
			DBInfo src_dbinfo=src_dbs.get(i);
			CollectWorker collect=new CollectWorker(src_dbinfo,source_db_query,target_table,target_db);
			//collect.collectData();
			threadpool.execute(collect);			
		}

	}
	
	/**
	* 收集数据主过程(循环运行)
	* @return
	*  
	*/
	static void collectDataLoop(int interval_seconds){
		isRun=true;
		while(isRun){
		for(int i=0;i<src_dbs.size();i++)
		{ 
			DBInfo src_dbinfo=src_dbs.get(i);
			CollectWorker collect=new CollectWorker(src_dbinfo,source_db_query,target_table,target_db);
			threadpool.execute(collect);			
		}
			try {
				MyUtil.log("--------------------------sleep "+String.valueOf(interval_seconds)+"seconds");
				Thread.sleep(interval_seconds*1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String current_path=System.getProperty("user.dir");
		System.out.println(current_path);
	    String cfgfile = "cfg.ini";
	    if (args.length > 0){
	      //MyUtil.log("args[0]:" + args[0]);
	      cfgfile = args[0];
	    }
		readcfg(cfgfile);
		if(once.indexOf("true")>=0){
		collectDataOnce();
		}
		else{
	     collectDataLoop(interval_seconds);
		}
		threadpool.shutdown();
		if(targetcon!=null)
		{
			try {
				targetcon.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
