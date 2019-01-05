/*
 * This file is part of LaS-VPE Platform.
 *
 * LaS-VPE Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cripac.isee.vpe.alg.pedestrian.reid;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.cripac.isee.alg.pedestrian.reid.Feature;
import org.cripac.isee.alg.pedestrian.reid.MSCANFeatureExtracter;
import org.cripac.isee.alg.pedestrian.reid.PedestrianInfo;
import org.cripac.isee.alg.pedestrian.reid.ReIDFeatureExtracter;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet.BoundingBox;
import org.cripac.isee.util.ObjectPool4Reid;
import org.cripac.isee.util.Singleton;
import org.cripac.isee.vpe.alg.pedestrian.tracking.TrackletOrURL;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.RobustExecutor;
import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.data.MysqlDaoJdbc;
import org.cripac.isee.vpe.entities.DataInfo;
import org.cripac.isee.vpe.util.logging.Logger;
import org.xml.sax.SAXException;

import scala.Tuple2;

/**
 * The PedestrianReIDFeatureExtractionApp class is a Spark Streaming application
 * which performs reid feature extraction.
 *
 * @author da.li, CRIPAC, 2017
 */
public class PedestrianReIDFeatureExtractionApp extends SparkStreamingApp {
    /**
     * The name of this application.
     */
    public static final String APP_NAME = "pedestrian-reid-feature-extraction";
    private static final long serialVersionUID = 7561012713161590005L;

    @Override
    public void addToContext() throws Exception {
        // Do nothing.
    }

    /**
     * Available algorithms of pedestrian reid feature extraction.
     */
    public enum ReIDAlgorithm {
        MSCAN,
    }

    /**
     * Constructor of the application, configuring properties read from a
     * property center.
     *
     * @param propCenter A class saving all the properties this application may need.
     * @throws Exception On failure in Spark.
     */
    public PedestrianReIDFeatureExtractionApp(AppPropertyCenter propCenter) throws Exception {
        super(propCenter, APP_NAME);

        registerStreams(Collections.singletonList(new ReIDFeatureExtractionStream(propCenter)));
    }

    public static class AppPropertyCenter extends SystemPropertyCenter {
        private static final long serialVersionUID = 7561912313161500905L;
        public ReIDAlgorithm algorithm = ReIDAlgorithm.MSCAN;

        public AppPropertyCenter(@Nonnull String[] args) 
            throws URISyntaxException, ParserConfigurationException, SAXException, UnknownHostException {
            super(args);
            // Digest the settings.
            for (Map.Entry<Object, Object> entry : sysProps.entrySet()) {
                switch ((String) entry.getKey()) {
                    case "vpe.ped.reid.alg":
                        algorithm = ReIDAlgorithm.valueOf((String) entry.getValue());
                        break;
                    case "driver.memory":
                        driverMem = (String) entry.getValue();
                        break;
                    case "executor.memory":
                        executorMem = (String) entry.getValue();
                        break;
                    default:
                        logger.warn("Unrecognized option: " + entry.getKey());
                        break;
                }
            }
        }
    } 

    /**
     * @param args No options supported currently.
     * @throws Exception On failure in Spark.
     */
    public static void main(String[] args) throws Exception {
        // Load system properties.
        AppPropertyCenter propCenter = new AppPropertyCenter(args);

        // Start the pedestrian tracking application.
        SparkStreamingApp app = new PedestrianReIDFeatureExtractionApp(propCenter);
        app.initialize();
        app.start();
        app.awaitTermination();
    }

    public static class ReIDFeatureExtractionStream extends Stream {

        public static final String NAME = "PedestrianReIDFeatureExtraction";
        public static final DataType OUTPUT_TYPE = DataType.REID_FEATURE;

        /**
         * Port to input pedestrian tracklets from Kafka.
         */
        public static final Port TRACKLET_PORT =
                new Port("pedestrian-tracklet-for-reid-feature-extraction",
                        DataType.TRACKLET);
        
        private static final long serialVersionUID = 3988152284961510251L;

        private Singleton<ReIDFeatureExtracter> reidSingleton;
        private Singleton<KeyedObjectPool<String,MSCANFeatureExtracter>> reidSingleton2;
        private Singleton<ObjectPool4Reid> reidSingleton3;
        
        
        public ReIDFeatureExtractionStream(AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            switch (propCenter.algorithm) {
                case MSCAN:
//                    reidSingleton = new Singleton<> (
//                        () -> new MSCANFeatureExtracter(propCenter.caffeGPU, loggerSingleton.getInst()),
//                        MSCANFeatureExtracter.class
//                    );
               reidSingleton3= new Singleton<>(() -> ObjectPool4Reid.getInstance(loggerSingleton,reportSingleton.getInst()),ObjectPool4Reid.class);
               reidSingleton2 = new Singleton<>(() -> reidSingleton3.getInst().pool,KeyedObjectPool.class);
            }
        }

        @Override
		public void addToGlobalStream(Map<DataType, JavaPairDStream<UUID, TaskData>> globalStreamMap) {

			// Extract tracklets from the data.
			JavaPairDStream<UUID, TaskData> trackletDStream = filter(globalStreamMap, TRACKLET_PORT);
			// Extract features from the tracklets.
			trackletDStream.foreachRDD(rdd -> {
				JavaRDD<List<Tuple2<UUID, TaskData>>> rddGlom = rdd.glom();
//				if (rddGlom.count() > 0) {
//
//					rddGlom.collect().forEach(list->{
//            			if (list!=null&&list.size()>0) {
//            				System.out.println("reid 开始----------------------------");
//							
//            				System.out.println("reid list的 大小是："+list.size());
//            				for (int i = 0; i <list.size(); i++) {
//            					TrackletOrURL trackletOrURL = (TrackletOrURL)list.get(i)._2().predecessorRes;
//            					try {
//									Tracklet tracklet = trackletOrURL.getTracklet();
//									System.out.println("reid要处理的trackletId是:"+tracklet.id);
//								} catch (Exception e) {
//									// TODO Auto-generated catch block
//									e.printStackTrace();
//								}
//							}
//            				System.out.println("需要处理的数据的个数是：" + rddGlom.count());
//						}
//            			});
//				}
				rddGlom.foreachPartition(iterator->{
					while(iterator.hasNext()){
						List<Tuple2<UUID, TaskData>> kvList=iterator.next();
//						long id=Thread.currentThread().getId();
						Logger logger = loggerSingleton.getInst();
						long startTime = System.currentTimeMillis();
						final long[] reidCostTime = { 0 };
						final int[] numSamples = { 0 };
						if (kvList.size() > 0) {
							logger.info("reid开始--------------:"+startTime);
//							logger.info("线程："+id+"，reid开始--------------:"+startTime);

							MysqlDaoJdbc mysqlDaoJdbc = mysqlSingleton.getInst();
							List<DataInfo> dataInfoList=new ArrayList<>();
							
							ObjectPool4Reid objectPool4Reid = reidSingleton3.getInst();
//							List<Integer> gpus=objectPool4Reid.gpus;
//							System.out.println("PedestrianReIDFeatureExtractionApp里的gpus是：" + gpus);
							ConcurrentHashMap<Integer,Integer> map=objectPool4Reid.map;
							KeyedObjectPool<String, MSCANFeatureExtracter> pool=reidSingleton2.getInst();
							ThreadLocal<Integer> tl = new ThreadLocal<Integer>();

							
							
							Integer pickGpu = null;
							if (tl.get()==null) {
								
								for (Integer getKey : map.keySet()) {
	//							Random random = new Random(System.currentTimeMillis());
	//								int size=map.size();
	//								int count=0;
	//								while (count<size) {
	//									
	//									Integer getKey=random.nextInt(size);
										if (map.get(getKey) == 0) {
											pickGpu = getKey;
											tl.set(pickGpu);
											break;
										}
	//									count++;
									}
	//								random=null;
								if ( pickGpu!=null) {
									
									map.replace(pickGpu,0, 1); 
								}else {
									System.out.println("key是空的");
								}
							}else {
								pickGpu=tl.get();
							}
							
							
							
//							if (pickGpu!=null) {
							String key =String.valueOf(pickGpu) ;
//							String key =String.valueOf(objectPool4Reid.selectGPURandomly(gpus)) ;
//							List<Integer> pickedGpuList=objectPool4Reid.pickGpu(map, 0);
//							String key =String.valueOf(objectPool4Reid.pickGpuByList(gpus,objectPool4Reid.selectGPURandomly(gpus))) ;
//							String key =String.valueOf(objectPool4Reid.randomlyPickGPU(gpus)) ;
//							Integer pickGpu=objectPool4Reid.pickGpu(map, 0);
							
//							System.out.println("PedestrianReIDFeatureExtractionApp使用的key是：" + key);
								
								long reidStartTime = System.currentTimeMillis();
								final MSCANFeatureExtracter mSCANFeatureExtracter =objectPool4Reid.getMSCANFeatureExtracter(key, pool);
//								final MSCANFeatureExtracter mSCANFeatureExtracter = (MSCANFeatureExtracter) reidSingleton.getInst();
								
//								boolean flag = true; 
								/*try {
//												mSCANFeatureExtracter = (MSCANFeatureExtracter) reidSingleton.getInst();
//												pool = reidSingleton2.getInst();
//												MSCANFeatureExtracter mSCANFeatureExtracter = pool.borrowObject(key);
//								mSCANFeatureExtracter = objectPool4Reid.getMSCANFeatureExtracter(key, pool);
								  
								//当前池里的实例数量  
//						        System.out.println("当前线程id："+current.getId()+",刚借出后，池中所有在用实例pool.getNumActive()："+reidSingleton2.getInst().getNumActive());  
//						        System.out.println("当前线程id："+current.getId()+",刚借出后，池中所有在用实例pool.getNumActive("+key+")："+reidSingleton2.getInst().getNumActive(key));  
//						        //当前池里的处于闲置状态的实例  
//						        System.out.println("当前线程id："+current.getId()+",刚借出后，池中处于闲置状态的实例pool.getNumIdle()："+reidSingleton2.getInst().getNumIdle());  
//						        System.out.println("当前线程id："+current.getId()+",刚借出后，池中处于闲置状态的实例pool.getNumIdle("+key+")："+reidSingleton2.getInst().getNumIdle(key));  
								} catch (Exception e) {
									// TODO: handle exception
									flag = false;  
						            e.printStackTrace();
								}*/
								long borrowObjectTime=System.currentTimeMillis()-reidStartTime;
								logger.info("PedestrianReIDFeatureExtractionApp使用的gpu是:"+ mSCANFeatureExtracter.gpu);
								
							kvList.forEach(kv -> {
								if (kv != null) {
									long startEachTime = System.currentTimeMillis();
									try {
											try {
												
												final UUID taskID = kv._1();
												final TaskData taskData = kv._2();
												logger.debug("To extract reid feature for task " + taskID + "!");
												DataInfo dataInfo = new DataInfo();
												dataInfo.setType("reid");
												dataInfo.setIsMultiGpu("Multi");
//												dataInfo.setIsMultiGpu("single");
												
												String nodeName=getServerName();
												dataInfo.setNodeName(nodeName);
												dataInfo.setStartEachTime(startEachTime);
												dataInfo.setTaskID(taskID.toString());
												
												dataInfo.setBorrowObjectTime(borrowObjectTime);
												dataInfo.setGpu(mSCANFeatureExtracter.gpu);
												dataInfo.setNodeName_gpu(nodeName+"-"+mSCANFeatureExtracter.gpu);
												dataInfo.setToString(mSCANFeatureExtracter.toString());
												dataInfo.setHashCode(System.identityHashCode(mSCANFeatureExtracter));
//												dataInfo.setIsException(0);
										
												// Etract features robustly;
												if (taskID != null && taskData != null) {
													TrackletOrURL trackletOrURL = (TrackletOrURL) taskData.predecessorRes;
													Tracklet tracklet =null;
													if (trackletOrURL!=null) {
													try {
															
															tracklet = trackletOrURL.getTracklet();
													} catch (Exception e) {
														// TODO: handle exception
														e.printStackTrace();
													}
//													logger.info("reid的trackletID是："+tracklet.id.toString());
													dataInfo.setTrackletId(tracklet.id.toString());
													dataInfo.setVideoName(tracklet.id.videoID);
													logger.debug("To extract reid feature for tracklet.id " + tracklet.id.toString() + "!");
													RobustExecutor<TrackletOrURL, Feature> robustExecutor = new RobustExecutor<>(
															(Function<TrackletOrURL, Feature>) tou -> {
																Feature fea =null;
																if (tou!=null&&(tou.getTracklet() != null || tou.getURL() != null)) {
																final PedestrianInfo pedestrian = new PedestrianInfo(tou);

																try {
																			
																	Tracklet t=tou.getTracklet();
																	if (t!=null&&t.locationSequence!=null&&t.locationSequence.length>0) {
																	fea =mSCANFeatureExtracter.extract(pedestrian);
																
																		 
																	long reidEndTime = System.currentTimeMillis();
																	reidCostTime[0] += reidEndTime - reidStartTime;
																		Collection<BoundingBox> list=t.getSamples();
																		numSamples[0] += list.size();
																	}else {
																		logger.debug("Tracklet为空,或者locationSequence为空!");
																	}
																
																} catch (Exception e) {
																	// TODO: handle exception
//																	dataInfo.setIsException(1);
																	e.printStackTrace();
																}
																}
																
																if (fea != null) {
																	
																	return fea;
																} else {
																	return null;
																}
																
															}
															
															);
													Feature feature = null;
													if (robustExecutor != null ) {

														feature = (Feature) robustExecutor.execute(trackletOrURL);
													}
													logger.debug("Feature retrieved for task " + taskID + "!");
													long endEachTime = System.currentTimeMillis();
													
													dataInfo.setEndEachTime(endEachTime);
													
//													if (dataInfo.getIsException()==0) {
														dataInfoList.add(dataInfo);
//													}
													// Find current node.
													final TaskData.ExecutionPlan.Node curNode = taskData.getDestNode(TRACKLET_PORT);
													// Get ports to output to.
													assert curNode != null;
													final List<TaskData.ExecutionPlan.Node.Port> outputPorts = curNode.getOutputPorts();
													// Mark the current node as
													// executed (Add userPlan by
													// da.li).
													curNode.markExecuted();
													if (feature != null && outputPorts != null
															&& taskData.executionPlan != null && taskData.userPlan != null
															&& taskID != null) {

														output(outputPorts, taskData.executionPlan, feature,
																taskData.userPlan, taskID);
													}
													}
												}
											} catch (Exception e) {
//												logger.error("During reid feature extraction.", e);
												e.printStackTrace();
											}
									} catch (Exception e) {
										e.printStackTrace();
									}
								}
							});
							try {  
//								if (flag) {  
									objectPool4Reid.releaseMSCANFeatureExtracter(key, mSCANFeatureExtracter, pool);
//									//当前池里的实例数量  
//						            System.out.println("当前线程id："+current.getId()+",release后，池中所有在用实例pool.getNumActive()："+reidSingleton2.getInst().getNumActive());  
//						            System.out.println("当前线程id："+current.getId()+",release后，池中所有在用实例pool.getNumActive("+key+")："+reidSingleton2.getInst().getNumActive(key));  
//						            //当前池里的处于闲置状态的实例  
//						            System.out.println("当前线程id："+current.getId()+",release后，池中处于闲置状态的实例pool.getNumIdle()："+reidSingleton2.getInst().getNumIdle());  
//						            System.out.println("当前线程id："+current.getId()+",release后，池中处于闲置状态的实例pool.getNumIdle("+key+")："+reidSingleton2.getInst().getNumIdle(key));  
//								} else {  
//									objectPool4Reid.invalidateMSCANFeatureExtracter(key, mSCANFeatureExtracter, pool);
//									//当前池里的实例数量  
//						            System.out.println("当前线程id："+current.getId()+",invalidate后，池中所有在用实例pool.getNumActive()："+reidSingleton2.getInst().getNumActive());  
//						            System.out.println("当前线程id："+current.getId()+",invalidate后，池中所有在用实例pool.getNumActive("+key+")："+reidSingleton2.getInst().getNumActive(key));  
//						            //当前池里的处于闲置状态的实例  
//						            System.out.println("当前线程id："+current.getId()+",invalidate后，池中处于闲置状态的实例pool.getNumIdle()："+reidSingleton2.getInst().getNumIdle());  
//						            System.out.println("当前线程id："+current.getId()+",invalidate后，池中处于闲置状态的实例pool.getNumIdle("+key+")："+reidSingleton2.getInst().getNumIdle(key));  
//								}  
							} catch (Exception e) {  
								e.printStackTrace();  
							}
							finally {
//								objectPool4Reid.updateMap(map,pickGpu);
								map.replace(pickGpu,1,0);
							}
							
							
							mysqlDaoJdbc.addReIdInfo(dataInfoList);
						}
						
						if (kvList.size() > 0) {
							long endTime = System.currentTimeMillis();
							logger.info("Overall speed=" + ((endTime - startTime) / kvList.size())
									+ "ms per tracklet (totally " + kvList.size() + " tracklets)");
							
//							logger.info("线程："+id+"，reid结束，每次所用时间是:"+(endTime-startTime));
//							accumulator.add(endTime-startTime);
//							System.out.println("线程："+id+",计时器里面的值:"+accumulator.value()+",count:"+accumulator.count()+",sum:"+accumulator.sum());
						}
						if (numSamples[0] > 0) {
							logger.info("Reid speed=" + (reidCostTime[0] / numSamples[0]) + "ms per sample (totally "
									+ numSamples[0] + " samples)");
						}
					
						
					}});


			});

		}

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(TRACKLET_PORT);
        }
        
        
    }
}
