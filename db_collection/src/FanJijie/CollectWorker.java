package FanJijie;
import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

//jdbc sql types
//ARRAY=2003
//BIGINT=-5
//BINARY=-2
//BIT=-7
//BLOB=2004
//CHAR=1
//CLOB=2005
//DATE=91
//DECIMAL=3
//DISTINCT=2001
//DOUBLE=8
//FLOAT=6
//INTEGER=4
//JAVA_OBJECT=2000
//LONGVARBINARY=-4
//LONGVARCHAR=-1
//NULL=0
//NUMERIC=2
//OTHER=1111
//REAL=7
//REF=2006
//SMALLINT=5
//STRUCT=2002
//TIME=92
//TIMESTAMP=93
//TINYINT=-6
//VARBINARY=-3
//VARCHAR=12

/**
* 从源库收集数据写入到汇集目标库，继承自Thread,用于放在线程池中工作
* @author Fanjijie
*  
*/
public class CollectWorker extends Thread {
	DBInfo mysource_dbinfo=null;
	String mysource_db_query="";
	String mytarget_table="";
	DBInfo mytarget_dbinfo=new DBInfo();

	//static variable
    static Connection targetcon=null;
	static Object target_conn_lock = new Object();  

	public CollectWorker(DBInfo src_dbinfo,String source_db_query,String target_table,DBInfo target_db){
		mysource_dbinfo=src_dbinfo;
		mysource_db_query=source_db_query; //采集SQL
		mytarget_table=target_table;       //目标表，与采集SQL字段列表一致
		mytarget_dbinfo=target_db;
	}
	
	//完成一次收集
    void collectData()
    {
    	Connection srccon = null;
		Statement srcst=null;
		ResultSet rs=null;
		ResultSetMetaData rsmeta=null;
		
    	synchronized(target_conn_lock){ 
    	if(targetcon == null){
    		try {
    			targetcon=mytarget_dbinfo.getConnection();
    		} catch (ClassNotFoundException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		} catch (SQLException e) {
    			System.out.println("connect to target database fault!");
    			e.printStackTrace();
    			return;
    		}
    		MyUtil.log("connect target database success!");
    		}
    	}
		try {
			srccon=mysource_dbinfo.getConnection();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			MyUtil.log("connect to source database fault!"+mysource_dbinfo.db_name);
			e.printStackTrace();
			return;
		}
		MyUtil.log("connect source database "+mysource_dbinfo.db_name+" success!");
		try {
			srcst=srccon.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		try {
			rs=srcst.executeQuery(mysource_db_query);
			rsmeta=rs.getMetaData();
			int col_count=rsmeta.getColumnCount();
			String insertsql="insert into  "+mytarget_table+" values (";
			for(int coli=0;coli<col_count;coli++){
				insertsql=insertsql+"?,";
			}
			insertsql=insertsql.substring(0, insertsql.length()-1);
			insertsql=insertsql+')';    	
			MyUtil.log("insertsql:"+insertsql);
			PreparedStatement targetpst=null;
			synchronized(target_conn_lock){ 
			targetpst=targetcon.prepareStatement(insertsql);
			}
			while(rs.next()){
				for(int coli=1;coli<=col_count;coli++){
					int  type=rsmeta.getColumnType(coli);
					switch(type){
					case 12://VARCHAR
						String varstr=rs.getString(coli);
						targetpst.setString(coli, varstr);
						break;
					case 1://CHAR
						varstr=rs.getString(coli);
						targetpst.setString(coli, varstr);
						break;
					case 9://DATE
				        
						Timestamp vardate=rs.getTimestamp(coli);
						targetpst.setTimestamp(coli, vardate);
						break;
					case 92://time
						vardate=rs.getTimestamp(coli);
						targetpst.setTimestamp(coli, vardate);
						break;
					case 93://TIMESTAMP
						vardate=rs.getTimestamp(coli);
						targetpst.setTimestamp(coli, vardate);
						break;
					case 2://NUMERIC
						double vardouble=rs.getDouble(coli);
						targetpst.setDouble(coli, vardouble);
						break;
					case 4://INTEGER
						int varint=rs.getInt(coli);
						targetpst.setInt(coli, varint);
						break;
					case 6://FLOAT 8DOUBLE
						float varfloat=rs.getFloat(coli);
						targetpst.setFloat(coli, varfloat);
						break;
					case 8:// 8DOUBLE
						 vardouble=rs.getDouble(coli);
						targetpst.setDouble(coli, vardouble);
						break;
					case 3:// DECIMAL
						 vardouble=rs.getDouble(coli);
						targetpst.setDouble(coli, vardouble);
						break;
					default:
						varstr=rs.getString(coli);
						targetpst.setString(coli, varstr);
					}
				}
				targetpst.execute();
			    MyUtil.log(mysource_dbinfo.db_name+":process 1 row!");

			}

			targetcon.commit();		
			srcst.close();
			targetpst.close();
			MyUtil.log(mysource_dbinfo.db_name+":target commit!");

			//srccon.close(); //暂不关闭链接，重复使用
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}

    }



	 @Override
	 public void run() {
		 collectData();
	 }

}
