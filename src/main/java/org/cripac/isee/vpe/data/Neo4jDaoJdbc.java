package org.cripac.isee.vpe.data;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.cripac.isee.alg.pedestrian.attr.Attributes;
import org.cripac.isee.alg.pedestrian.reid.Feature;
import org.cripac.isee.vpe.entities.Data4Neo4j;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Values;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Neo4jDaoJdbc implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -6272364250663999916L;
	private static final Logger log = Logger.getLogger(Neo4jDaoJdbc.class);// 日志文件
	
	public static void main(String[] args) {
		
	}

	public void setPedestrianReIDFeature(@Nonnull String nodeID, @Nonnull String dataType, @Nonnull Feature fea) {
		byte[] feature = fea.getBytes();
		String feaStringBase64 = Base64.encodeBase64String(feature);
		String sql ="MERGE (p:Person {trackletID: {1}, dataType: {2}}) SET " + "p.reidFeature={3}, "
				+ "p.isFinish={4}, " + "p.isGetSim={5};";
		PreparedStatement ps = null;
		Connection conn = null;
		long startTime = System.currentTimeMillis();
		try {
			conn = JdbcUtils4Neo4j.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setString(1, nodeID);
			ps.setString(2, dataType);
			ps.setString(3, feaStringBase64);
			ps.setBoolean(4, true);
			ps.setBoolean(5, false);
			boolean result=ps.execute();
			System.out.println("插入数据库执行结果："+result);
		} catch (Exception e) {
			// TODO: handle exception
		}finally {
			JdbcUtils4Neo4j.releaseConnection(conn);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("保存的时间是:"+(endTime-startTime));
	}
	
	public void setPedestrianReIDFeature(List<Data4Neo4j> list,String lableName) {
		
		String sql ="MERGE (p:Person:"+lableName+" {trackletID: {1}, dataType: {2}}) SET " + "p.reidFeature={3}, "
				+ "p.isFinish={4}, " + "p.isGetSim={5};";
		PreparedStatement ps = null;
		Connection conn = null;
		long startTime = System.currentTimeMillis();
		conn = JdbcUtils4Neo4j.getConnection();
		long getEndTime=System.currentTimeMillis();
//		System.out.println("得到对象所需时间："+(getEndTime-startTime));
		try {
			JdbcUtils4Neo4j.beginTransaction(conn);
			ps = conn.prepareStatement(sql);
//			System.out.println("list的大小是："+list.size());
			for (int i = 0; i < list.size(); i++) {
				Data4Neo4j data4Neo4j=list.get(i);
				String nodeID=data4Neo4j.getNodeID();
				String dataType=data4Neo4j.getDataType();
				Feature fea=data4Neo4j.getFea();
				
				byte[] feature = fea.getBytes();
				String feaStringBase64 = Base64.encodeBase64String(feature);
				ps.setString(1, nodeID);
				ps.setString(2, dataType);
				ps.setString(3, feaStringBase64);
				ps.setBoolean(4, true);
				ps.setBoolean(5, false);
				ps.addBatch();
			}
			int[] count = ps.executeBatch();
			JdbcUtils4Neo4j.commitTransaction(conn);
			System.out.println("reid执行成功的个数是:" + count.length);
			ps.clearBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			JdbcUtils4Neo4j.rollbackTransaction(conn);
			for (int i = 0; i < list.size(); i++) {
				System.out.println("reid 保存失败:"+list.get(i).getNodeID());
			}
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

			JdbcUtils4Neo4j.releaseConnection(conn);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("reid保存的时间是:"+(endTime-getEndTime)+"毫秒");
	}
	
	public void setPedestrianTracklet(List<Data4Neo4j> list,String lableName) {
		// Insert Node.
		String insertSql ="MERGE (p:Person:"+lableName+" {trackletID: {1}, dataType: {2}}) SET " + "p.path={3}, "
						+ "p.startTime=toint({4}), " + "p.startIndex={5}, "
						+ "p.boundingBoxes={6}, " + "p.camID={7}, " + "p.videoURL={8};";
				
				
		
				
		PreparedStatement ps = null;
		Connection conn = null;
		long startTime = System.currentTimeMillis();
		
		try {
			conn = JdbcUtils4Neo4j.getConnection();
			JdbcUtils4Neo4j.beginTransaction(conn);
			ps = conn.prepareStatement(insertSql);
			
			
			for (int i = 0; i < list.size(); i++) {
				Data4Neo4j data4Neo4j=list.get(i);
				String nodeID=data4Neo4j.getNodeID();
				String dataType=data4Neo4j.getDataType();
				String trackletPath=data4Neo4j.getTrackletPath();
				String trackletInfo=data4Neo4j.getTrackletInfo();
				// Parse tracklet information.
				JsonParser jParser = new JsonParser();
				JsonObject jObject = jParser.parse(trackletInfo).getAsJsonObject();
				// Start frame index of a tracklet.
				int trackletStartIdx = jObject.get("run-frame-index").getAsInt();
				// tracklet id.
				JsonObject jObjectId = jObject.get("id").getAsJsonObject();
				// Start time & CAM ID.
				String videoURL = jObjectId.get("video-url").getAsString();
				String camID = videoURL.split("-")[0];
				String videoStartTime = videoURL.split("-")[1];
				String trackletStartTime = calTrackletStartTime(trackletStartIdx, videoStartTime);
				// bounding box
				JsonArray jArrayBoundingBoxes = jObject.get("bounding-boxes").getAsJsonArray();
				String bbCoordinatesInfo = jArrayBoundingBoxes.toString();
				
//				ps.setString(1, lableName);
//				ps.setString(2, camID);
				ps.setString(1, nodeID);
				ps.setString(2, dataType);
				ps.setString(3, trackletPath);
				ps.setString(4, trackletStartTime);
				ps.setInt(5, trackletStartIdx);
				ps.setString(6, bbCoordinatesInfo);
				ps.setString(7, camID);
				ps.setString(8, videoURL);
				
//				System.out.println("打印sql："+ps.toString());
				ps.addBatch();
				
			}
			int[] count = ps.executeBatch();
			JdbcUtils4Neo4j.commitTransaction(conn);
			System.out.println("tracking执行成功的个数是:" + count.length);
			ps.clearBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			JdbcUtils4Neo4j.rollbackTransaction(conn);
			for (int i = 0; i < list.size(); i++) {
				System.out.println("tracking 保存失败:"+list.get(i).getNodeID());
			}
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
			JdbcUtils4Neo4j.releaseConnection(conn);
		}
		
		long endTime = System.currentTimeMillis();
		System.out.println("tracking保存到neo4j的时间是:"+(endTime-startTime)+"毫秒");
		
		
		

	}
	
	public boolean setTrackletRelation(List<Data4Neo4j> list, String timeTreeLable) {
				
				
		// Insert Relation. About 400ms ...
				
		String insertRelationSql ="MATCH (n:Root)-[:HAS_YEAR]->(y:Year {year: toint({1})})-[:HAS_MONTH]->"
						+ "(mon:Month {month: toint({2})})-[:HAS_DAY]->(d:Day {day: toint({3})})-[:HAS_HOUR]->"
						+ "(h:Hour {hour: toint({4})})-[:HAS_MIN]->(min:Minute:"+timeTreeLable+") WHERE toint(tostring(min.start)+'00')<="
						+ "toint({5}) AND toint({6})<=toint(tostring(min.end)+'59') "
						+ "MATCH (p:Person {trackletID: {7}, dataType: {8}}) MERGE (min)-[:INCLUDES_PERSON]->(p);";
				
		PreparedStatement ps2 = null;
		Connection conn = null;
		long startTime = System.currentTimeMillis();
		
		boolean b=false;
		
		try {
			conn = JdbcUtils4Neo4j.getConnection();
			JdbcUtils4Neo4j.beginTransaction(conn);
			
			ps2 = conn.prepareStatement(insertRelationSql);
			
			for (int i = 0; i < list.size(); i++) {
				Data4Neo4j data4Neo4j=list.get(i);
				String nodeID=data4Neo4j.getNodeID();
				String dataType=data4Neo4j.getDataType();
				String trackletInfo=data4Neo4j.getTrackletInfo();
				// Parse tracklet information.
				JsonParser jParser = new JsonParser();
				JsonObject jObject = jParser.parse(trackletInfo).getAsJsonObject();
				// Start frame index of a tracklet.
				int trackletStartIdx = jObject.get("run-frame-index").getAsInt();
				// tracklet id.
				JsonObject jObjectId = jObject.get("id").getAsJsonObject();
				// Start time & CAM ID.
				String videoURL = jObjectId.get("video-url").getAsString();
				String videoStartTime = videoURL.split("-")[1];
				String trackletStartTime = calTrackletStartTime(trackletStartIdx, videoStartTime);
				
				
				String queryYear = trackletStartTime.substring(0, 4);
				String queryMon = trackletStartTime.substring(0, 6);
				String queryDay = trackletStartTime.substring(0, 8);
				String queryHour = trackletStartTime.substring(0, 10);
				
				ps2.setString(1, queryYear);
				ps2.setString(2, queryMon);
				ps2.setString(3, queryDay);
				ps2.setString(4, queryHour);
				ps2.setString(5, trackletStartTime);
				ps2.setString(6, trackletStartTime);
				ps2.setString(7, nodeID);
				ps2.setString(8, dataType);
				ps2.addBatch();
			}
			int[] count2 = ps2.executeBatch();
			JdbcUtils4Neo4j.commitTransaction(conn);
			System.out.println("关系执行成功的个数是:" + count2.length);
			ps2.clearBatch();
			b=true;
			return b;
		
		} catch (Exception e) {
			// TODO: handle exception
			JdbcUtils4Neo4j.rollbackTransaction(conn);
			System.out.println("关系 保存失败");
			for (int i = 0; i < list.size(); i++) {
				System.out.println("关系 失败:"+list.get(i).getNodeID());
			}
			e.printStackTrace();
			
		}finally {

			if (ps2 != null) {
				
				try {
					ps2.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			JdbcUtils4Neo4j.releaseConnection(conn);
		
		}
		long endTime2 = System.currentTimeMillis();
		System.out.println("关系保存到neo4j的时间是:"+(endTime2-startTime)+"毫秒");
		return b;
		

	}
	
	public void setPedestrianTracklet(@Nonnull String nodeID, @Nonnull String dataType, @Nonnull String trackletPath,
			@Nonnull String trackletInfo) {
		// Parse tracklet information.
		JsonParser jParser = new JsonParser();
		JsonObject jObject = jParser.parse(trackletInfo).getAsJsonObject();
		// Start frame index of a tracklet.
		int trackletStartIdx = jObject.get("run-frame-index").getAsInt();
		// tracklet id.
		JsonObject jObjectId = jObject.get("id").getAsJsonObject();
		// Start time & CAM ID.
		String videoURL = jObjectId.get("video-url").getAsString();
		String camID = videoURL.split("-")[0];
		String videoStartTime = videoURL.split("-")[1];
		String trackletStartTime = calTrackletStartTime(trackletStartIdx, videoStartTime);
		// bounding box
		JsonArray jArrayBoundingBoxes = jObject.get("bounding-boxes").getAsJsonArray();
		String bbCoordinatesInfo = jArrayBoundingBoxes.toString();

		// Insert Node.
		String insertSql ="MERGE (p:Person {trackletID: {1}, dataType: {2}}) SET " + "p.path={3}, "
				+ "p.startTime=toint({4}), " + "p.startIndex={5}, "
				+ "p.boundingBoxes={6}, " + "p.camID={7}, " + "p.videoURL={8};";
		
		
		// Insert Relation. About 400ms ...
		/*
		*/
		String queryYear = trackletStartTime.substring(0, 4);
		String queryMon = trackletStartTime.substring(0, 6);
		String queryDay = trackletStartTime.substring(0, 8);
		String queryHour = trackletStartTime.substring(0, 10);
		
		String insertRelationSql ="MATCH (n:Root)-[:HAS_YEAR]->(y:Year {year: toint({1})})-[:HAS_MONTH]->"
				+ "(mon:Month {month: toint({2})})-[:HAS_DAY]->(d:Day {day: toint({3})})-[:HAS_HOUR]->"
				+ "(h:Hour {hour: toint({4})})-[:HAS_MIN]->(min) WHERE toint(tostring(min.start)+'00')<="
				+ "toint({5}) AND toint({6})<=toint(tostring(min.end)+'59') "
				+ "MATCH (p:Person {trackletID: {7}, dataType: {8}}) MERGE (min)-[:INCLUDES_PERSON]->(p);";
		
		
		PreparedStatement ps = null;
		Connection conn = null;
		long startTime = System.currentTimeMillis();
		conn = JdbcUtils4Neo4j.getConnection();
		try {
			ps = conn.prepareStatement(insertSql);
			ps.setString(1, nodeID);
			ps.setString(2, dataType);
			ps.setString(3, trackletPath);
			ps.setString(4, trackletStartTime);
			ps.setInt(5, trackletStartIdx);
			ps.setString(6, bbCoordinatesInfo);
			ps.setString(7, camID);
			ps.setString(8, videoURL);
			boolean node=ps.execute();
			System.out.println("插入数据库执行结果："+node);
			
			ps = conn.prepareStatement(insertRelationSql);
			ps.setString(1, queryYear);
			ps.setString(2, queryMon);
			ps.setString(3, queryDay);
			ps.setString(4, queryHour);
			ps.setString(5, trackletStartTime);
			ps.setString(6, trackletStartTime);
			ps.setString(7, nodeID);
			ps.setString(8, dataType);
			boolean result=ps.execute();
			System.out.println("插入关系时，数据库执行结果："+result);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		finally {
			JdbcUtils4Neo4j.releaseConnection(conn);
		}
		
		long endTime = System.currentTimeMillis();
		System.out.println("保存的node的时间是:"+(endTime-startTime));
		
		

	}

	public void setPedestrianAttributes(List<Data4Neo4j> list,String lableName){
		String sql ="MERGE (p:Person:"+lableName+" {trackletID: {1}, dataType: {2}}) SET "
                + "p.genderMale = {3}, "
                + "p.genderFemale = {4}, "
                + "p.genderOther = {5}, "
                + "p.ageSixteen = {6}, "
                + "p.ageThirty = {7}, "
                + "p.ageFortyFive = {8}, "
                + "p.ageSixty = {9}, "
                + "p.ageOlderSixty = {10}, "
                + "p.weightVeryFat = {11}, "
                + "p.weightLittleFat = {12}, "
                + "p.weightNormal = {13}, "
                + "p.weightLittleThin = {14}, "
                + "p.weightVeryThin = {15}, "
                + "p.roleClient = {16}, "
                + "p.roleUniform = {17}, "
                + "p.hairStyleNull = {18}, "
                + "p.hairStyleLong = {19}, "
                + "p.headShoulderBlackHair = {20}, "
                + "p.headShoulderWithHat = {21}, "
                + "p.headShoulderGlasses = {22}, "
                + "p.headShoulderSunglasses = {23}, "
                + "p.headShoulderScarf = {24}, "
                + "p.headShoulderMask = {25}, "
                + "p.upperShirt = {26}, "
                + "p.upperSweater = {27}, "
                + "p.upperVest = {28}, "
                + "p.upperTshirt = {29}, "
                + "p.upperCotton = {30}, "
                + "p.upperJacket = {31}, "
                + "p.upperSuit = {32}, "
                + "p.upperHoodie = {33}, "
                + "p.upperCotta = {34}, "
                + "p.upperOther = {35}, "
                + "p.upperBlack = {36}, "
                + "p.upperWhite = {37}, "
                + "p.upperGray = {38}, "
                + "p.upperRed = {39}, "
                + "p.upperGreen = {40}, "
                + "p.upperBlue = {41}, "
                + "p.upperSilvery = {42}, "
                + "p.upperYellow = {43}, "
                + "p.upperBrown = {44}, "
                + "p.upperPurple = {45}, "
                + "p.upperPink = {46}, "
                + "p.upperOrange = {47}, "
                + "p.upperMixColor = {48}, "
                + "p.upperOtherColor = {49}, "
                + "p.lowerPants = {50}, "
                + "p.lowerShortPants = {51}, "
                + "p.lowerSkirt = {52}, "
                + "p.lowerShortSkirt = {53}, "
                + "p.lowerLongSkirt = {54}, "
                + "p.lowerOnePiece = {55}, "
                + "p.lowerJean = {56}, "
                + "p.lowerTightPants = {57}, "
                + "p.lowerBlack = {58}, "
                + "p.lowerWhite = {59}, "
                + "p.lowerGray = {60}, "
                + "p.lowerRed = {61}, "
                + "p.lowerGreen = {62}, "
                + "p.lowerBlue = {63}, "
                + "p.lowerSilver = {64}, "
                + "p.lowerYellow = {65}, "
                + "p.lowerBrown = {66}, "
                + "p.lowerPurple = {67}, "
                + "p.lowerPink = {68}, "
                + "p.lowerOrange = {69}, "
                + "p.lowerMixColor = {70}, "
                + "p.lowerOtherColor = {71}, "
                + "p.shoesLeather = {72}, "
                + "p.shoesSport = {73}, "
                + "p.shoesBoot = {74}, "
                + "p.shoesCloth = {75}, "
                + "p.shoesShandle = {76}, "
                + "p.shoesCasual = {77}, "
                + "p.shoesOther = {78}, "
                + "p.shoesBlack = {79}, "
                + "p.shoesWhite = {80}, "
                + "p.shoesGray = {81}, "
                + "p.shoesRed = {82}, "
                + "p.shoesGreen = {83}, "
                + "p.shoesBlue = {84}, "
                + "p.shoesSilver = {85}, "
                + "p.shoesYellow = {86}, "
                + "p.shoesBrown = {87}, "
                + "p.shoesPurple = {88}, "
                + "p.shoesPink = {89}, "
                + "p.shoesOrange = {90}, "
                + "p.shoesMixColor = {91}, "
                + "p.shoesOtherColor = {92}, "
                + "p.accessoryBackpack = {93}, "
                + "p.accessoryShoulderBag = {94}, "
                + "p.accessoryHandBag = {95}, "
                + "p.accessoryWaistBag = {96}, "
                + "p.accessoryBox = {97}, "
                + "p.accessoryPlasticBag = {98}, "
                + "p.accessoryPaperBag = {99}, "
                + "p.accessoryCart = {100}, "
                + "p.accessoryKid = {101}, "
                + "p.accessoryOther = {102}, "
                + "p.actionCalling = {103}, "
                + "p.actionArmStretching = {104}, "
                + "p.actionChatting = {105}, "
                + "p.actionGathering = {106}, "
                + "p.actionLying = {107}, "
                + "p.actionCrouching = {108}, "
                + "p.actionRunning = {109}, "
                + "p.actionHoldThing = {110}, "
                + "p.actionPushing = {111}, "
                + "p.actionPulling = {112}, "
                + "p.actionNipThing = {113}, "
                + "p.actionPicking = {114}, "
                + "p.actionOther = {115}, "
                + "p.viewAngleLeft = {116}, "
                + "p.viewAngleRight = {117}, "
                + "p.viewAngleFront = {118}, "
                + "p.viewAngleBack = {119}, "
                + "p.occlusionLeft = {120}, "
                + "p.occlusionRight = {121}, "
                + "p.occlusionUp = {122}, "
                + "p.occlusionDown = {123}, "
                + "p.occlusionEnvironment = {124}, "
                + "p.occlusionAccessory = {125}, "
                + "p.occlusionObject = {126}, "
                + "p.occlusionOther = {127};";
    	
		PreparedStatement ps = null;
		Connection conn = null;
		long startTime = System.currentTimeMillis();
		try {
			conn = JdbcUtils4Neo4j.getConnection();
			JdbcUtils4Neo4j.beginTransaction(conn);
			ps = conn.prepareStatement(sql);
//			System.out.println("list 的大小是："+list.size());
			for (int i = 0; i < list.size(); i++) {
				Data4Neo4j data4Neo4j=list.get(i);
				String nodeID=data4Neo4j.getNodeID();
				System.out.println("for nodeId:"+nodeID);
				String dataType=data4Neo4j.getDataType();
				Attributes attr=data4Neo4j.getAttr();
				ps.setString(1, nodeID);
				ps.setString(2, dataType);
				ps.setFloat(3, attr.genderMale);
				ps.setFloat(4, attr.genderFemale);
				ps.setFloat(5, attr.genderOther);
				ps.setFloat(6, attr.ageSixteen);
				ps.setFloat(7, attr.ageThirty);
				ps.setFloat(8, attr.ageFortyFive);
				ps.setFloat(9, attr.ageSixty);
				ps.setFloat(10, attr.ageOlderSixty);
				ps.setFloat(11, attr.weightVeryFat);
				ps.setFloat(12, attr.weightLittleFat);
				ps.setFloat(13, attr.weightNormal);
				ps.setFloat(14, attr.weightLittleThin);
				ps.setFloat(15, attr.weightVeryThin);
				ps.setFloat(16, attr.roleClient);
				ps.setFloat(17, attr.roleUniform);
				ps.setFloat(18, attr.hairStyleNull);
				ps.setFloat(19, attr.hairStyleLong);
				ps.setFloat(20, attr.headShoulderBlackHair);
				ps.setFloat(21, attr.headShoulderWithHat);
				ps.setFloat(22, attr.headShoulderGlasses);
				ps.setFloat(23, attr.headShoulderSunglasses);
				ps.setFloat(24, attr.headShoulderScarf);
				ps.setFloat(25, attr.headShoulderMask);
				ps.setFloat(26, attr.upperShirt);
				ps.setFloat(27, attr.upperSweater);
				ps.setFloat(28, attr.upperVest);
				ps.setFloat(29, attr.upperTshirt);
				ps.setFloat(30, attr.upperCotton);
				ps.setFloat(31, attr.upperJacket);
				ps.setFloat(32, attr.upperSuit);
				ps.setFloat(33, attr.upperHoodie);
				ps.setFloat(34, attr.upperCotta);
				ps.setFloat(35, attr.upperOther);
				ps.setFloat(36, attr.upperBlack);
				ps.setFloat(37, attr.upperWhite);
				ps.setFloat(38, attr.upperGray);
				ps.setFloat(39, attr.upperRed);
				ps.setFloat(40, attr.upperGreen);
				ps.setFloat(41, attr.upperBlue);
				ps.setFloat(42, attr.upperSilvery);
				ps.setFloat(43, attr.upperYellow);
				ps.setFloat(44, attr.upperBrown);
				ps.setFloat(45, attr.upperPurple);
				ps.setFloat(46, attr.upperPink);
				ps.setFloat(47, attr.upperOrange);
				ps.setFloat(48, attr.upperMixColor);
				ps.setFloat(49, attr.upperOtherColor);
				ps.setFloat(50, attr.lowerPants);
				ps.setFloat(51, attr.lowerShortPants);
				ps.setFloat(52, attr.lowerSkirt);
				ps.setFloat(53, attr.lowerShortSkirt);
				ps.setFloat(54, attr.lowerLongSkirt);
				ps.setFloat(55, attr.lowerOnePiece);
				ps.setFloat(56, attr.lowerJean);
				ps.setFloat(57, attr.lowerTightPants);
				ps.setFloat(58, attr.lowerBlack);
				ps.setFloat(59, attr.lowerWhite);
				ps.setFloat(60, attr.lowerGray);
				ps.setFloat(61, attr.lowerRed);
				ps.setFloat(62, attr.lowerGreen);
				ps.setFloat(63, attr.lowerBlue);
				ps.setFloat(64, attr.lowerSilver);
				ps.setFloat(65, attr.lowerYellow);
				ps.setFloat(66, attr.lowerBrown);
				ps.setFloat(67, attr.lowerPurple);
				ps.setFloat(68, attr.lowerPink);
				ps.setFloat(69, attr.lowerOrange);
				ps.setFloat(70, attr.lowerMixColor);
				ps.setFloat(71, attr.lowerOtherColor);
				ps.setFloat(72, attr.shoesLeather);
				ps.setFloat(73, attr.shoesSport);
				ps.setFloat(74, attr.shoesBoot);
				ps.setFloat(75, attr.shoesCloth);
				ps.setFloat(76, attr.shoesShandle);
				ps.setFloat(77, attr.shoesCasual);
				ps.setFloat(78, attr.shoesOther);
				ps.setFloat(79, attr.shoesBlack);
				ps.setFloat(80, attr.shoesWhite);
				ps.setFloat(81, attr.shoesGray);
				ps.setFloat(82, attr.shoesRed);
				ps.setFloat(83, attr.shoesGreen);
				ps.setFloat(84, attr.shoesBlue);
				ps.setFloat(85, attr.shoesSilver);
				ps.setFloat(86, attr.shoesYellow);
				ps.setFloat(87, attr.shoesBrown);
				ps.setFloat(88, attr.shoesPurple);
				ps.setFloat(89, attr.shoesPink);
				ps.setFloat(90, attr.shoesOrange);
				ps.setFloat(91, attr.shoesMixColor);
				ps.setFloat(92, attr.shoesOtherColor);
				ps.setFloat(93, attr.accessoryBackpack);
				ps.setFloat(94, attr.accessoryShoulderBag);
				ps.setFloat(95, attr.accessoryHandBag);
				ps.setFloat(96, attr.accessoryWaistBag);
				ps.setFloat(97, attr.accessoryBox);
				ps.setFloat(98, attr.accessoryPlasticBag);
				ps.setFloat(99, attr.accessoryPaperBag);
				ps.setFloat(100, attr.accessoryCart);
				ps.setFloat(101, attr.accessoryKid);
				ps.setFloat(102, attr.accessoryOther);
				ps.setFloat(103, attr.actionCalling);
				ps.setFloat(104, attr.actionArmStretching);
				ps.setFloat(105, attr.actionChatting);
				ps.setFloat(106, attr.actionGathering);
				ps.setFloat(107, attr.actionLying);
				ps.setFloat(108, attr.actionCrouching);
				ps.setFloat(109, attr.actionRunning);
				ps.setFloat(110, attr.actionHoldThing);
				ps.setFloat(111, attr.actionPushing);
				ps.setFloat(112, attr.actionPulling);
				ps.setFloat(113, attr.actionNipThing);
				ps.setFloat(114, attr.actionPicking);
				ps.setFloat(115, attr.actionOther);
				ps.setFloat(116, attr.viewAngleLeft);
				ps.setFloat(117, attr.viewAngleRight);
				ps.setFloat(118, attr.viewAngleFront);
				ps.setFloat(119, attr.viewAngleBack);
				ps.setFloat(120, attr.occlusionLeft);
				ps.setFloat(121, attr.occlusionRight);
				ps.setFloat(122, attr.occlusionUp);
				ps.setFloat(123, attr.occlusionDown);
				ps.setFloat(124, attr.occlusionEnvironment);
				ps.setFloat(125, attr.occlusionAccessory);
				ps.setFloat(126, attr.occlusionObject);
				ps.setFloat(127, attr.occlusionOther);
				ps.addBatch();
			}
			int[] count = ps.executeBatch();
			JdbcUtils4Neo4j.commitTransaction(conn);
			System.out.println("属性识别执行成功的个数是:" + count.length);
			ps.clearBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			JdbcUtils4Neo4j.rollbackTransaction(conn);
			for (int i = 0; i < list.size(); i++) {
				System.out.println("属性识别 保存失败:"+list.get(i).getNodeID());
			}
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

			JdbcUtils4Neo4j.releaseConnection(conn);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("属性识别保存neo4j的时间是:"+(endTime-startTime)+"毫秒");
	}
    public void setPedestrianAttributes(@Nonnull String nodeID, 
                                        @Nonnull String dataType,
                                        @Nonnull Attributes attr) {
        // Set attributes to an existing node or one newly created.
    	String sql ="MERGE (p:Person {trackletID: {1}, dataType: {2}}) SET "
                + "p.genderMale = {3}, "
                + "p.genderFemale = {4}, "
                + "p.genderOther = {5}, "
                + "p.ageSixteen = {6}, "
                + "p.ageThirty = {7}, "
                + "p.ageFortyFive = {8}, "
                + "p.ageSixty = {9}, "
                + "p.ageOlderSixty = {10}, "
                + "p.weightVeryFat = {11}, "
                + "p.weightLittleFat = {12}, "
                + "p.weightNormal = {13}, "
                + "p.weightLittleThin = {14}, "
                + "p.weightVeryThin = {15}, "
                + "p.roleClient = {16}, "
                + "p.roleUniform = {17}, "
                + "p.hairStyleNull = {18}, "
                + "p.hairStyleLong = {19}, "
                + "p.headShoulderBlackHair = {20}, "
                + "p.headShoulderWithHat = {21}, "
                + "p.headShoulderGlasses = {22}, "
                + "p.headShoulderSunglasses = {23}, "
                + "p.headShoulderScarf = {24}, "
                + "p.headShoulderMask = {25}, "
                + "p.upperShirt = {26}, "
                + "p.upperSweater = {27}, "
                + "p.upperVest = {28}, "
                + "p.upperTshirt = {29}, "
                + "p.upperCotton = {30}, "
                + "p.upperJacket = {31}, "
                + "p.upperSuit = {32}, "
                + "p.upperHoodie = {33}, "
                + "p.upperCotta = {34}, "
                + "p.upperOther = {35}, "
                + "p.upperBlack = {36}, "
                + "p.upperWhite = {37}, "
                + "p.upperGray = {38}, "
                + "p.upperRed = {39}, "
                + "p.upperGreen = {40}, "
                + "p.upperBlue = {41}, "
                + "p.upperSilvery = {42}, "
                + "p.upperYellow = {43}, "
                + "p.upperBrown = {44}, "
                + "p.upperPurple = {45}, "
                + "p.upperPink = {46}, "
                + "p.upperOrange = {47}, "
                + "p.upperMixColor = {48}, "
                + "p.upperOtherColor = {49}, "
                + "p.lowerPants = {50}, "
                + "p.lowerShortPants = {51}, "
                + "p.lowerSkirt = {52}, "
                + "p.lowerShortSkirt = {53}, "
                + "p.lowerLongSkirt = {54}, "
                + "p.lowerOnePiece = {55}, "
                + "p.lowerJean = {56}, "
                + "p.lowerTightPants = {57}, "
                + "p.lowerBlack = {58}, "
                + "p.lowerWhite = {59}, "
                + "p.lowerGray = {60}, "
                + "p.lowerRed = {61}, "
                + "p.lowerGreen = {62}, "
                + "p.lowerBlue = {63}, "
                + "p.lowerSilver = {64}, "
                + "p.lowerYellow = {65}, "
                + "p.lowerBrown = {66}, "
                + "p.lowerPurple = {67}, "
                + "p.lowerPink = {68}, "
                + "p.lowerOrange = {69}, "
                + "p.lowerMixColor = {70}, "
                + "p.lowerOtherColor = {71}, "
                + "p.shoesLeather = {72}, "
                + "p.shoesSport = {73}, "
                + "p.shoesBoot = {74}, "
                + "p.shoesCloth = {75}, "
                + "p.shoesShandle = {76}, "
                + "p.shoesCasual = {77}, "
                + "p.shoesOther = {78}, "
                + "p.shoesBlack = {79}, "
                + "p.shoesWhite = {80}, "
                + "p.shoesGray = {81}, "
                + "p.shoesRed = {82}, "
                + "p.shoesGreen = {83}, "
                + "p.shoesBlue = {84}, "
                + "p.shoesSilver = {85}, "
                + "p.shoesYellow = {86}, "
                + "p.shoesBrown = {87}, "
                + "p.shoesPurple = {88}, "
                + "p.shoesPink = {89}, "
                + "p.shoesOrange = {90}, "
                + "p.shoesMixColor = {91}, "
                + "p.shoesOtherColor = {92}, "
                + "p.accessoryBackpack = {93}, "
                + "p.accessoryShoulderBag = {94}, "
                + "p.accessoryHandBag = {95}, "
                + "p.accessoryWaistBag = {96}, "
                + "p.accessoryBox = {97}, "
                + "p.accessoryPlasticBag = {98}, "
                + "p.accessoryPaperBag = {99}, "
                + "p.accessoryCart = {100}, "
                + "p.accessoryKid = {101}, "
                + "p.accessoryOther = {102}, "
                + "p.actionCalling = {103}, "
                + "p.actionArmStretching = {104}, "
                + "p.actionChatting = {105}, "
                + "p.actionGathering = {106}, "
                + "p.actionLying = {107}, "
                + "p.actionCrouching = {108}, "
                + "p.actionRunning = {109}, "
                + "p.actionHoldThing = {110}, "
                + "p.actionPushing = {111}, "
                + "p.actionPulling = {112}, "
                + "p.actionNipThing = {113}, "
                + "p.actionPicking = {114}, "
                + "p.actionOther = {115}, "
                + "p.viewAngleLeft = {116}, "
                + "p.viewAngleRight = {117}, "
                + "p.viewAngleFront = {118}, "
                + "p.viewAngleBack = {119}, "
                + "p.occlusionLeft = {120}, "
                + "p.occlusionRight = {121}, "
                + "p.occlusionUp = {122}, "
                + "p.occlusionDown = {123}, "
                + "p.occlusionEnvironment = {124}, "
                + "p.occlusionAccessory = {125}, "
                + "p.occlusionObject = {126}, "
                + "p.occlusionOther = {127};";
    	
		PreparedStatement ps = null;
		Connection conn = null;
		long startTime = System.currentTimeMillis();
		conn = JdbcUtils4Neo4j.getConnection();
		try {
			ps = conn.prepareStatement(sql);
			ps.setString(1, nodeID);
			ps.setString(2, dataType);
			ps.setFloat(3, attr.genderMale);
			ps.setFloat(4, attr.genderFemale);
			ps.setFloat(5, attr.genderOther);
			ps.setFloat(6, attr.ageSixteen);
			ps.setFloat(7, attr.ageThirty);
			ps.setFloat(8, attr.ageFortyFive);
			ps.setFloat(9, attr.ageSixty);
			ps.setFloat(10, attr.ageOlderSixty);
			ps.setFloat(11, attr.weightVeryFat);
			ps.setFloat(12, attr.weightLittleFat);
			ps.setFloat(13, attr.weightNormal);
			ps.setFloat(14, attr.weightLittleThin);
			ps.setFloat(15, attr.weightVeryThin);
			ps.setFloat(16, attr.roleClient);
			ps.setFloat(17, attr.roleUniform);
			ps.setFloat(18, attr.hairStyleNull);
			ps.setFloat(19, attr.hairStyleLong);
			ps.setFloat(20, attr.headShoulderBlackHair);
			ps.setFloat(21, attr.headShoulderWithHat);
			ps.setFloat(22, attr.headShoulderGlasses);
			ps.setFloat(23, attr.headShoulderSunglasses);
			ps.setFloat(24, attr.headShoulderScarf);
			ps.setFloat(25, attr.headShoulderMask);
			ps.setFloat(26, attr.upperShirt);
			ps.setFloat(27, attr.upperSweater);
			ps.setFloat(28, attr.upperVest);
			ps.setFloat(29, attr.upperTshirt);
			ps.setFloat(30, attr.upperCotton);
			ps.setFloat(31, attr.upperJacket);
			ps.setFloat(32, attr.upperSuit);
			ps.setFloat(33, attr.upperHoodie);
			ps.setFloat(34, attr.upperCotta);
			ps.setFloat(35, attr.upperOther);
			ps.setFloat(36, attr.upperBlack);
			ps.setFloat(37, attr.upperWhite);
			ps.setFloat(38, attr.upperGray);
			ps.setFloat(39, attr.upperRed);
			ps.setFloat(40, attr.upperGreen);
			ps.setFloat(41, attr.upperBlue);
			ps.setFloat(42, attr.upperSilvery);
			ps.setFloat(43, attr.upperYellow);
			ps.setFloat(44, attr.upperBrown);
			ps.setFloat(45, attr.upperPurple);
			ps.setFloat(46, attr.upperPink);
			ps.setFloat(47, attr.upperOrange);
			ps.setFloat(48, attr.upperMixColor);
			ps.setFloat(49, attr.upperOtherColor);
			ps.setFloat(50, attr.lowerPants);
			ps.setFloat(51, attr.lowerShortPants);
			ps.setFloat(52, attr.lowerSkirt);
			ps.setFloat(53, attr.lowerShortSkirt);
			ps.setFloat(54, attr.lowerLongSkirt);
			ps.setFloat(55, attr.lowerOnePiece);
			ps.setFloat(56, attr.lowerJean);
			ps.setFloat(57, attr.lowerTightPants);
			ps.setFloat(58, attr.lowerBlack);
			ps.setFloat(59, attr.lowerWhite);
			ps.setFloat(60, attr.lowerGray);
			ps.setFloat(61, attr.lowerRed);
			ps.setFloat(62, attr.lowerGreen);
			ps.setFloat(63, attr.lowerBlue);
			ps.setFloat(64, attr.lowerSilver);
			ps.setFloat(65, attr.lowerYellow);
			ps.setFloat(66, attr.lowerBrown);
			ps.setFloat(67, attr.lowerPurple);
			ps.setFloat(68, attr.lowerPink);
			ps.setFloat(69, attr.lowerOrange);
			ps.setFloat(70, attr.lowerMixColor);
			ps.setFloat(71, attr.lowerOtherColor);
			ps.setFloat(72, attr.shoesLeather);
			ps.setFloat(73, attr.shoesSport);
			ps.setFloat(74, attr.shoesBoot);
			ps.setFloat(75, attr.shoesCloth);
			ps.setFloat(76, attr.shoesShandle);
			ps.setFloat(77, attr.shoesCasual);
			ps.setFloat(78, attr.shoesOther);
			ps.setFloat(79, attr.shoesBlack);
			ps.setFloat(80, attr.shoesWhite);
			ps.setFloat(81, attr.shoesGray);
			ps.setFloat(82, attr.shoesRed);
			ps.setFloat(83, attr.shoesGreen);
			ps.setFloat(84, attr.shoesBlue);
			ps.setFloat(85, attr.shoesSilver);
			ps.setFloat(86, attr.shoesYellow);
			ps.setFloat(87, attr.shoesBrown);
			ps.setFloat(88, attr.shoesPurple);
			ps.setFloat(89, attr.shoesPink);
			ps.setFloat(90, attr.shoesOrange);
			ps.setFloat(91, attr.shoesMixColor);
			ps.setFloat(92, attr.shoesOtherColor);
			ps.setFloat(93, attr.accessoryBackpack);
			ps.setFloat(94, attr.accessoryShoulderBag);
			ps.setFloat(95, attr.accessoryHandBag);
			ps.setFloat(96, attr.accessoryWaistBag);
			ps.setFloat(97, attr.accessoryBox);
			ps.setFloat(98, attr.accessoryPlasticBag);
			ps.setFloat(99, attr.accessoryPaperBag);
			ps.setFloat(100, attr.accessoryCart);
			ps.setFloat(101, attr.accessoryKid);
			ps.setFloat(102, attr.accessoryOther);
			ps.setFloat(103, attr.actionCalling);
			ps.setFloat(104, attr.actionArmStretching);
			ps.setFloat(105, attr.actionChatting);
			ps.setFloat(106, attr.actionGathering);
			ps.setFloat(107, attr.actionLying);
			ps.setFloat(108, attr.actionCrouching);
			ps.setFloat(109, attr.actionRunning);
			ps.setFloat(110, attr.actionHoldThing);
			ps.setFloat(111, attr.actionPushing);
			ps.setFloat(112, attr.actionPulling);
			ps.setFloat(113, attr.actionNipThing);
			ps.setFloat(114, attr.actionPicking);
			ps.setFloat(115, attr.actionOther);
			ps.setFloat(116, attr.viewAngleLeft);
			ps.setFloat(117, attr.viewAngleRight);
			ps.setFloat(118, attr.viewAngleFront);
			ps.setFloat(119, attr.viewAngleBack);
			ps.setFloat(120, attr.occlusionLeft);
			ps.setFloat(121, attr.occlusionRight);
			ps.setFloat(122, attr.occlusionUp);
			ps.setFloat(123, attr.occlusionDown);
			ps.setFloat(124, attr.occlusionEnvironment);
			ps.setFloat(125, attr.occlusionAccessory);
			ps.setFloat(126, attr.occlusionObject);
			ps.setFloat(127, attr.occlusionOther);
			boolean result=ps.execute();
			System.out.println("插入数据库执行结果："+result);
		} catch (Exception e) {
			// TODO: handle exception
		}finally {
			JdbcUtils4Neo4j.releaseConnection(conn);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("保存的时间是:"+(endTime-startTime));
		
        
    }
    
	private String calTrackletStartTime(@Nonnull int trackletStartIdx, @Nonnull String videoStartTime) {
		final int fpsDenominator = 2;
		final int fpsNumerator = 25;
		final int secPos = 12;
		final int minPos = 10;

		int trackletDuration = trackletStartIdx * fpsDenominator / fpsNumerator;
		// Confirm second:
		int secOld = 0;
		try {
			secOld = Integer.parseInt(videoStartTime.substring(secPos));
		} catch (NumberFormatException e) {
		}
		int min = (secOld + trackletDuration) / 60;
		int sec = (secOld + trackletDuration) % 60;
		String secNew = null;
		if (sec < 10) {
			secNew = "0" + sec;
		} else {
			secNew = "" + sec;
		}
		// Confirm minute:
		int minOld = 0;
		try {
			minOld = Integer.parseInt(videoStartTime.substring(minPos, secPos));
		} catch (NumberFormatException e) {
		}
		int hour = (minOld + min) / 60;
		min = (minOld + min) % 60;
		String minNew = null;
		if (min < 10) {
			minNew = "0" + min;
		} else {
			minNew = "" + min;
		}
		// Confirm hour:
		int hourOld = 0;
		try {
			hourOld = Integer.parseInt(videoStartTime.substring(0, minPos));
		} catch (NumberFormatException e) {
		}
		hour = hourOld + hour;

		// Finally, get start time of a tracklet.
		String trackletStartTime = "" + hour + minNew + secNew;

		return trackletStartTime;
	}
}
