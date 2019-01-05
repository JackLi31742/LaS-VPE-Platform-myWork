package org.cripac.isee.vpe.data;

import javax.sql.DataSource;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DataSourceSingleton {
	// 饿汉式
//		private static DataSource ds = new ComboPooledDataSource();
	private static ComboPooledDataSource ds = new ComboPooledDataSource();
		
//		static{
//			InputStream inStream = DataSourceSingleton.class.getResourceAsStream("/c3p0-config.xml");
//			inStream.
//			 System.setProperty("com.mchange.v2.c3p0.cfg.xml",);
//		}
		//静态初始化块进行初始化  
	    static{  
	        try {  
	              
	            ds.setDriverClass("com.mysql.jdbc.Driver");//设置连接池连接数据库所需的驱动  
	              
	            ds.setJdbcUrl("jdbc:mysql://172.18.33.7:3306/test"
	            		+ "?autoReconnect=true&rewriteBatchedStatements=true&allowMultiQueries=true");//设置连接数据库的URL  
	              
	            ds.setUser("root");//设置连接数据库的用户名  
	              
	            ds.setPassword("_cgWMaL<?0Jt");//设置连接数据库的密码  
	              
	            ds.setMaxPoolSize(15);//设置连接池的最大连接数  
	              
	            ds.setMinPoolSize(3);//设置连接池的最小连接数  
	              
	            ds.setInitialPoolSize(3);//设置连接池的初始连接数  
	              
	            ds.setMaxStatements(20);//设置连接池的缓存Statement的最大数              
	        } catch (Exception e) {  
	            e.printStackTrace();  
	        }  
	    }  
		public static DataSource getDataSource() {
			return ds;
		}
		private DataSourceSingleton(){}
//		public static void main(String[] args) {
//			System.out.println(this.getClass().getResource("/c3p0-config.xml"));
//		}
}
