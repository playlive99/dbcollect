package FanJijie;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
* 数据库连接信息类
* @author Fanjijie
*  
*/
public class DBInfo {

	public DBInfo(){

	}
	public DBInfo(String db_name,String ip,String port,String service_name,String username,String pwd){
		this.db_name=db_name;

	}
	public String db_name;
	public String ip;
	public String port;
	public String service_name;
	public String username;
	public String pwd;
	private Connection conn=null;
	
	/**
	* 验证连接是否有效
	* @return boolean 连接是否有效
	*  
	*/
	private boolean isValid()
	{
		boolean ret=false;
		if(conn==null){
			return false;
		}
		try{
			Statement st=conn.createStatement();
			st.executeQuery("select sysdate from dual");
			st.close();
			ret=true;
		}
		catch(Exception ex)
		{
			MyUtil.log("isValid:"+ex.toString());
			ret=false;
		}
		return ret;
		
	}
	
	/**
	* 获取数据库连接
	* @return Connection 返回数据库连接
	*  
	*/
	public synchronized  Connection getConnection() throws ClassNotFoundException, SQLException
	{
		 if(conn == null||!isValid()){
		 String driver = "oracle.jdbc.driver.OracleDriver";//database driver
	 	 String url = "jdbc:oracle:thin:@"+ip+":"+port+"/"+service_name;//database URL
		 String user = username; 
		 String password = pwd;       //database Password
 		 Class.forName(driver);
		 conn = DriverManager.getConnection(url, user, password);
		 conn.setAutoCommit(false);
		}
		return conn;  		
	}
}
