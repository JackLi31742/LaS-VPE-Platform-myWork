package org.cripac.isee.vpe.data;

import javax.sql.DataSource;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DataSourceSingleton4Neo4j {
private static ComboPooledDataSource ds = new ComboPooledDataSource();
	
	//静态初始化块进行初始化  
    static{  
        try {  
              
            ds.setDriverClass("org.neo4j.jdbc.Driver");//设置连接池连接数据库所需的驱动  
              
            ds.setJdbcUrl("jdbc:neo4j:bolt://172.18.33.8:7687");//设置连接数据库的URL  
              
            ds.setUser("neo4j");//设置连接数据库的用户名  
              
            ds.setPassword("casia@1234");//设置连接数据库的密码  
              
            ds.setMaxPoolSize(20);//设置连接池的最大连接数  
              
            ds.setMinPoolSize(2);//设置连接池的最小连接数  
              
            ds.setInitialPoolSize(10);//设置连接池的初始连接数  
              
            ds.setMaxStatements(100);//设置连接池的缓存Statement的最大数              
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
    }  
	public static DataSource getDataSource() {
		return ds;
	}
	private DataSourceSingleton4Neo4j(){}
}
