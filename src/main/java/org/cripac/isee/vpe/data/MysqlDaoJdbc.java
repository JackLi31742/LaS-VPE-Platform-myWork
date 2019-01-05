package org.cripac.isee.vpe.data;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.log4j.Logger;
import org.cripac.isee.vpe.entities.DataInfo;

import scala.Tuple3;


public class MysqlDaoJdbc implements Serializable{
	private static final long serialVersionUID = 2736910077813555175L;
	private static final Logger log = Logger.getLogger(MysqlDaoJdbc.class);// 日志文件

	public MysqlDaoJdbc() {
	}

	//属性识别和reid跑的tracklet少了
	public static List<String> cha(String sqlString){
		PreparedStatement ps = null;
//		String sqlString = "select t.trackletId FROM trackInfo as t where  t.startEachTime>'2018-04-25 15:30:30'";
		Connection conn = null;
		conn = JdbcUtils.getConnection();
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(sqlString);
			rs = ps.executeQuery();
			List<String> list = new ArrayList<String>();
			while (rs.next()) {
				list.add(rs.getString(1));
			}
			return list;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			JdbcUtils.releaseConnection(conn);
		}
		return null;
	}
	

	public static void main(String[] args) throws ParseException  {
//		String trsql = "select t.trackletId FROM trackInfo as t where  t.startEachTime>'2018-04-25 15:30:30'";
//		String attsql = "select a.trackletId  from attrInfo as a where  a.startEachTime>'2018-04-25 15:30:30'";
//		String reidsql = "select t.trackletId FROM reIdInfo as t where  t.startEachTime>'2018-04-25 15:30:30'";
//		
//		Collection<String> result=CollectionUtils.subtract(cha(trsql), cha(reidsql)); 
//		Collection<String> result1=CollectionUtils.subtract(cha(trsql), cha(attsql)); 
//		Collection<String> result2=CollectionUtils.subtract(result, result1); 
//		
//		result2.forEach(f->{System.out.println(f);});
		countTime();
	}
	
	public static void countTime()throws ParseException{
//		List<Person> list =query();
//		System.out.println(list);
		//统计时长
		List<String> list=query();
		long count=0;
		for (int i = 0; i < list.size(); i++) {
			String s=list.get(i);
			String[] arr=s.split("-");
			SimpleDateFormat sdf =   new SimpleDateFormat( "yyyyMMddHHmmss" ); 
			Date date2 = sdf.parse( arr[2] );
			Date date1 = sdf.parse( arr[1] );
//			String str = sdf.format(date2);
			long a=date2.getTime()-date1.getTime();
			System.out.println(i+","+s+","+a);
			count=a+count;
		}
		System.out.println(count);
	}
	public static void count(){
		PreparedStatement ps = null;
		String sqlString = "select * from trackInfo WHERE trackInfo.startEachTime>'2018-04-04 16:18:18'";
		Connection conn = null;
		conn = JdbcUtils.getConnection();
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(sqlString);
			rs = ps.executeQuery();
			List<Person> list = new ArrayList<Person>();
			while (rs.next()) {
				Person person = new Person();
				person.setName(rs.getString(1));
				person.setAge(rs.getInt(2));
				list.add(person);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			JdbcUtils.releaseConnection(conn);
		}
	}
	public static List<String> query() {
		PreparedStatement ps = null;
		String sqlString = "select DISTINCT trackInfo.videoName from trackInfo WHERE trackInfo.startEachTime>'2018-08-02 08:56:23'";
		Connection conn = null;
		conn = JdbcUtils.getConnection();
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(sqlString);
			rs = ps.executeQuery();
			List<String> list = new ArrayList<String>();
			while (rs.next()) {
				
				String s=rs.getString(1);
				list.add(s);
			}
			return list;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			JdbcUtils.releaseConnection(conn);
		}
		return null;

	}
	
	public static void add(){

		PreparedStatement ps = null;
//		String sqlString = "INSERT INTO person(name,age) values (?,?)";
//		String sqlString = "INSERT INTO info(taskID,videoName,type,startEachTime,endEachTime)  
//		values ('dd3d5904-0f51-4871-8cdb-b68c752554d8','CAM01-20131223124239-20131223124831','tracking',1520558465481,1520558465481)";
		String sqlString = "INSERT INTO info(taskID,videoName,type,startEachTime,endEachTime) values (?,?,?,?,?)";
		Connection conn = null;
		conn = JdbcUtils.getConnection();
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(sqlString);
//				for (int i = 0; i < 5; i++) {
//					
//					ps.setString(1, "a"+i);
//					ps.setInt(2, 2);
//					ps.addBatch();
//				}
//				int[] count = ps.executeBatch();
//				System.out.println("执行成功的个数是:" + count.length);
			ps.setString(1, "dd3d5904-0f51-4871-8cdb-b68c752554d8");
			ps.setString(2, "CAM01-20131223124239-20131223124831");
			ps.setString(3, "tracking");
			ps.setTimestamp(4, new Timestamp(1520558465481l));
			ps.setTimestamp(5, new Timestamp(1520558465481l));
			ps.execute();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			JdbcUtils.releaseConnection(conn);
		}

	
	}
	public void addReIdInfo(List<DataInfo> dataInfoList) {
		PreparedStatement ps = null;
		String sql =  "INSERT INTO reIdInfo (taskID,videoName,type,nodeName,trackletId,gpu,nodeName_gpu,toString,hashCode,"
				+ "isMultiGpu,startEachTime,endEachTime,borrowObjectTime,end_start,pid) "
				+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		Connection conn = null;
		long startTime = System.currentTimeMillis();
		try {
			conn = JdbcUtils.getConnection();
			ps = conn.prepareStatement(sql);
			System.out.println("reid 要保存的list大小是:"+dataInfoList.size());
			for (int i = 0; i < dataInfoList.size(); i++) {
				DataInfo dataInfo=dataInfoList.get(i);	
//				System.out.println("要保存的数据是："+dataInfo.toString());
				ps.setString(1, dataInfo.getTaskID());
				ps.setString(2, dataInfo.getVideoName());
				ps.setString(3, dataInfo.getType());
				ps.setString(4, dataInfo.getNodeName());
				ps.setString(5, dataInfo.getTrackletId());
				ps.setString(6, dataInfo.getGpu());
				ps.setString(7, dataInfo.getNodeName_gpu());
				ps.setString(8, dataInfo.getToString());
				ps.setInt(9, dataInfo.getHashCode());
				ps.setString(10, dataInfo.getIsMultiGpu());
				ps.setTimestamp(11, new Timestamp(dataInfo.getStartEachTime()));
				ps.setTimestamp(12, new Timestamp(dataInfo.getEndEachTime()));
				ps.setLong(13, dataInfo.getBorrowObjectTime());
				ps.setLong(14, dataInfo.getEnd_start());
				ps.setLong(15, dataInfo.getPid());
//				ps.setInt(16, dataInfo.getIsException());
				ps.addBatch();
			}
			int[] count = ps.executeBatch();
			System.out.println("执行成功的个数是:" + count.length);
			ps.clearBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (ps != null) {
				
				try {
					ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			JdbcUtils.releaseConnection(conn);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("保存的时间是:"+(endTime-startTime)+"毫秒");
	}
	
	public void addReIdInfo(DataInfo dataInfo) {
		PreparedStatement ps = null;
		String sql =  "INSERT INTO reIdInfo (taskID,videoName,type,nodeName,trackletId,gpu,nodeName_gpu,toString,hashCode,isMultiGpu,startEachTime,endEachTime,borrowObjectTime) "
				+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
		Connection conn = null;
		long startTime = System.currentTimeMillis();
		try {
			conn = JdbcUtils.getConnection();
			ps = conn.prepareStatement(sql);
			System.out.println("要保存的数据是："+dataInfo.toString());
			ps.setString(1, dataInfo.getTaskID());
			ps.setString(2, dataInfo.getVideoName());
			ps.setString(3, dataInfo.getType());
			ps.setString(4, dataInfo.getNodeName());
			ps.setString(5, dataInfo.getTrackletId());
			ps.setString(6, dataInfo.getGpu());
			ps.setString(7, dataInfo.getNodeName_gpu());
			ps.setString(8, dataInfo.getToString());
			ps.setInt(9, dataInfo.getHashCode());
			ps.setString(10, dataInfo.getIsMultiGpu());
			ps.setTimestamp(11, new Timestamp(dataInfo.getStartEachTime()));
			ps.setTimestamp(12, new Timestamp(dataInfo.getEndEachTime()));
			ps.setLong(13, dataInfo.getBorrowObjectTime());
			boolean result=ps.execute();
			System.out.println("插入数据库执行结果："+result);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			JdbcUtils.releaseConnection(conn);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("保存的时间是:"+(endTime-startTime));
	}
	
	public void addAttrInfo(List<DataInfo> dataInfoList) {
		PreparedStatement ps = null;
		String sql =  "INSERT INTO attrInfo (taskID,videoName,type,nodeName,trackletId,gpu,nodeName_gpu,toString,hashCode,"
				+ "isMultiGpu,startEachTime,endEachTime,borrowObjectTime,end_start,pid) "
				+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		Connection conn = null;
		long startTime = System.currentTimeMillis();
		try {
			conn = JdbcUtils.getConnection();
			ps = conn.prepareStatement(sql);
			System.out.println("属性识别 要保存的list大小是:"+dataInfoList.size());
			for (int i = 0; i < dataInfoList.size(); i++) {
				DataInfo dataInfo=dataInfoList.get(i);	
//				System.out.println("要保存的数据是："+dataInfo.toString());
				ps.setString(1, dataInfo.getTaskID());
				ps.setString(2, dataInfo.getVideoName());
				ps.setString(3, dataInfo.getType());
				ps.setString(4, dataInfo.getNodeName());
				ps.setString(5, dataInfo.getTrackletId());
				ps.setString(6, dataInfo.getGpu());
				ps.setString(7, dataInfo.getNodeName_gpu());
				ps.setString(8, dataInfo.getToString());
				ps.setInt(9, dataInfo.getHashCode());
				ps.setString(10, dataInfo.getIsMultiGpu());
				ps.setTimestamp(11, new Timestamp(dataInfo.getStartEachTime()));
				ps.setTimestamp(12, new Timestamp(dataInfo.getEndEachTime()));
				ps.setLong(13, dataInfo.getBorrowObjectTime());
				ps.setLong(14, dataInfo.getEnd_start());
				ps.setLong(15, dataInfo.getPid());
//				ps.setInt(16, dataInfo.getIsException());
				ps.addBatch();
			}
			int[] count = ps.executeBatch();
			System.out.println("执行成功的个数是:" + count.length);
			ps.clearBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (ps != null) {
				
				try {
					ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			JdbcUtils.releaseConnection(conn);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("保存的时间是:"+(endTime-startTime)+"毫秒");
	}
	
	public void addAttrInfo(DataInfo dataInfo) {
		PreparedStatement ps = null;
		String sql =  "INSERT INTO attrInfo (taskID,videoName,type,nodeName,trackletId,gpu,nodeName_gpu,toString,hashCode,isMultiGpu,startEachTime,endEachTime,borrowObjectTime) "
				+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
		Connection conn = null;
		long startTime = System.currentTimeMillis();
		try {
			conn = JdbcUtils.getConnection();
			ps = conn.prepareStatement(sql);
			System.out.println("要保存的数据是："+dataInfo.toString());
			ps.setString(1, dataInfo.getTaskID());
			ps.setString(2, dataInfo.getVideoName());
			ps.setString(3, dataInfo.getType());
			ps.setString(4, dataInfo.getNodeName());
			ps.setString(5, dataInfo.getTrackletId());
			ps.setString(6, dataInfo.getGpu());
			ps.setString(7, dataInfo.getNodeName_gpu());
			ps.setString(8, dataInfo.getToString());
			ps.setInt(9, dataInfo.getHashCode());
			ps.setString(10, dataInfo.getIsMultiGpu());
			ps.setTimestamp(11, new Timestamp(dataInfo.getStartEachTime()));
			ps.setTimestamp(12, new Timestamp(dataInfo.getEndEachTime()));
			ps.setLong(13, dataInfo.getBorrowObjectTime());
			boolean result=ps.execute();
			System.out.println("插入数据库执行结果："+result);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			JdbcUtils.releaseConnection(conn);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("保存的时间是:"+(endTime-startTime));
	}
	
	public void addTrackingInfo(DataInfo dataInfo){
		PreparedStatement ps = null;
		String sql = "INSERT INTO trackInfo(trackletId,taskID,videoName,trackletSize,type,nodeName,gpu,nodeName_gpu,startEachTime,endEachTime) "
				+ "values (?,?,?,?,?,?,?,?,?,?)";
		Connection conn = null;
		long startTime = System.currentTimeMillis();
		try {
			conn = JdbcUtils.getConnection();
			ps = conn.prepareStatement(sql);
			System.out.println("要保存的数据是："+dataInfo.toString());
			ps.setString(1, dataInfo.getTrackletId());
			ps.setString(2, dataInfo.getTaskID());
			ps.setString(3, dataInfo.getVideoName());
			ps.setInt(4, dataInfo.getTrackletSize());
			ps.setString(5, dataInfo.getType());
			ps.setString(6, dataInfo.getNodeName());
			ps.setString(7, dataInfo.getGpu());
			ps.setString(8, dataInfo.getNodeName_gpu());
			ps.setTimestamp(9, new Timestamp(dataInfo.getStartEachTime()));
			ps.setTimestamp(10, new Timestamp(dataInfo.getEndEachTime()));
			boolean result=ps.execute();
			System.out.println("插入数据库执行结果："+result);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			JdbcUtils.releaseConnection(conn);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("保存的时间是:"+(endTime-startTime));
	}
	
	public int addSimRel(
			Iterator<scala.collection.immutable.List<Tuple3<String, scala.Double, String>>> iterator
			,String level
			) {
		long dbstartTime = System.currentTimeMillis();
		PreparedStatement ps = null;
		int countAll=0;
		int num=0;
		try {
			if (iterator != null) {
				while (iterator.hasNext()) {
					scala.collection.immutable.List<Tuple3<String, scala.Double, String>> list = iterator.next();
					if (list != null) {
						if (list.size() > 0 || list.length() > 0) {
							System.out.println("该次需要保存的list的大小是：" + list.size() + "," + list.length());
							try {
								Connection conn =null;
								conn = JdbcUtils.getConnection();
								if (level.equals("minute")) {
									
//									ps = conn.prepareStatement(min_sql);
								}if (level.equals("hour")) {
									
//									ps = conn.prepareStatement(hour_sql);
								}
								for (int i = 0; i < list.size(); i++) {
									Tuple3<String, scala.Double, String> tuple = list.apply(i);
									String nodeID1 = tuple._1();
									String nodeID2 = tuple._3();
									double SimRel = java.lang.Double.valueOf(tuple._2() + "");
									System.out.println("jdbc min需要保存的结果是：[{'sim':" + SimRel + ",'trackletID1':'"
											+ nodeID1 + "','trackletID2':'" + nodeID2 + "'}]");
									ps.setString(1, nodeID1);
									ps.setString(2, nodeID2);
									ps.setDouble(3, SimRel);
									ps.addBatch();
								}
								int[] count = ps.executeBatch();
								System.out.println("执行成功的个数是:" + count.length);
								countAll += count.length;
								ps.clearBatch();
								if (ps != null) {

									ps.close();
								} 
								JdbcUtils.releaseConnection(conn);
							} catch (Exception e) {
								// TODO: handle exception
//								e.printStackTrace();
							}
							finally {
								try {
								} catch (Exception e) {
									// TODO Auto-generated catch block
//									e.printStackTrace();
								}
							}

						} else {
							System.out.println("Iterator list 的大小是0");
						}
					} else {
						System.out.println("Iterator list is null");
					}
				num++;
				}
			} else {
				System.out.println("Iterator is null");
			}
		} catch (Exception e) {
			// TODO: handle exception
//			e.printStackTrace();
		} finally {
			// TODO: handle finally clause
			try {
			} catch (Exception e) {
				// TODO Auto-generated catch block
//				e.printStackTrace();
			}
		}
		long dbendTime = System.currentTimeMillis();
		System.out.println("循环了多少次:"+num+",该次需要保存的数量是："+countAll  + ",Cost everytime of addSimRel of minute : " + (dbendTime - dbstartTime) + "ms");
		return countAll;

	}
}
