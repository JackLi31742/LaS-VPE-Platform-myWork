package org.cripac.isee.vpe.data;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class JdbcUtils4Neo4j {

	/**
	 * 它为null表示没有事务 它不为null表示有事务 当开启事务时，需要给它赋值 当结束事务时，需要给它赋值为null
	 * 并且在开启事务时，让dao的多个方法共享这个Connection
	 */
	private static ThreadLocal<Connection> tl = new ThreadLocal<Connection>();
	private static DataSource ds=DataSourceSingleton4Neo4j.getDataSource();

	/**
	 * dao使用本方法来获取连接
	 * 
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnection() {
		
		Connection con = tl.get();
		if (con != null) {
			System.out.println("ThreadLocal获取连接成功！"+con.hashCode());
			return con;
		} else {
			try {
				con = ds.getConnection();
				 tl.set(con);  
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("ds获取连接成功！"+con.hashCode());
			return con;
		}
	}

	/**
	 * 开启事务
	 * 
	 * @throws SQLException
	 */
	public static void beginTransaction() {
		Connection con = tl.get();// 获取当前线程的事务连接
		if (con != null) {
			try {
				throw new SQLException("已经开启了事务，不能重复开启！");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {

			try {
				con = ds.getConnection();// 给con赋值，表示开启了事务
				con.setAutoCommit(false);// 设置为手动提交
				tl.set(con);// 把当前事务连接放到tl中
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	/**
	 * 提交事务
	 * 
	 * @throws SQLException
	 */
	public static void commitTransaction() {
		Connection con = tl.get();// 获取当前线程的事务连接
		if (con == null) {
			try {
				throw new SQLException("没有事务不能提交！");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {

			try {
				con.commit();
				con.close();// 关闭连接
				con = null;// 表示事务结束！
				tl.remove();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} // 提交事务
		}
	}

	/**
	 * 回滚事务
	 * 
	 * @throws SQLException
	 */
	public static void rollbackTransaction() {
		Connection con = tl.get();// 获取当前线程的事务连接
		if (con == null)
			try {
				throw new SQLException("没有事务不能回滚！");

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		else {

			try {
				con.rollback();
				con.close();
				con = null;
				tl.remove();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 释放Connection
	 * 
	 * @param con
	 * @throws SQLException
	 */
	public static void releaseConnection(Connection connection) {
		Connection con = tl.get();// 获取当前线程的事务连接
		try {
			/*
			 * 判断它是不是事务专用，如果是，就不关闭！
			 * 如果不是事务专用，那么就要关闭！
			 */
			// 如果con == null，说明现在没有事务，那么connection一定不是事务专用的！
//			if(con == null) {
//				connection.close();
//				}
//			// 如果con != null，说明有事务，那么需要判断参数连接是否与con相等，若不等，说明参数连接不是事务专用连接
//			if(con != connection) {
//				connection.close();
//			}
			if (con==connection) {
				connection.close();
				tl.remove();
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	/** 
     * 事务开启 
     * @return 
     */  
    public static void beginTransaction(Connection conn) {  
        try {  
            if (conn != null) {  
                if (conn.getAutoCommit()) {  
                    conn.setAutoCommit(false); //手动提交  
                }  
            }  
        }catch(SQLException e) {
        	e.printStackTrace();
        }  
    }  
      
    /** 
     * 事务提交 
     * @return 
     */  
    public static void commitTransaction(Connection conn) {  
        try {  
            if (conn != null) {  
                if (!conn.getAutoCommit()) {  
                    conn.commit();  
                }  
            }  
        }catch(SQLException e) {
        	e.printStackTrace();
        }  
    }  
      
    /** 
     * 事务回滚 
     * @return 
     */  
    public static void rollbackTransaction(Connection conn) {  
        try {  
            if (conn != null) {  
                if (!conn.getAutoCommit()) {  
                    conn.rollback();  
                }  
            }  
        }catch(SQLException e) {
        	e.printStackTrace();
        }  
    }     
	
	public static void main(String[] args) {
		Connection con =getConnection();
		System.out.println(con.hashCode());
		releaseConnection(con);
		Connection con1 =getConnection();
		System.out.println(con1.hashCode());
	}
}
