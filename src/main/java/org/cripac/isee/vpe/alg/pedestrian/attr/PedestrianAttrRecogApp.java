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

package org.cripac.isee.vpe.alg.pedestrian.attr;

import java.net.InetAddress;
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

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.cripac.isee.alg.pedestrian.attr.Attributes;
import org.cripac.isee.alg.pedestrian.attr.DeepMARCaffeNative;
import org.cripac.isee.alg.pedestrian.attr.ExternRecognizer;
import org.cripac.isee.alg.pedestrian.attr.Recognizer;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet.BoundingBox;
import org.cripac.isee.util.ObjectPool4DeepMARCaffeNative;
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
 * The PedestrianAttrRecogApp class is a Spark Streaming application which
 * performs pedestrian attribute recognition.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class PedestrianAttrRecogApp extends SparkStreamingApp {
	/**
	 * The name of this application.
	 */
	public static final String APP_NAME = "pedestrian-attr-recog";
    private static final long serialVersionUID = 2559258492435661197L;

    @Override
    public void addToContext() throws Exception {
        // Do nothing.
    }

    /**
     * Available algorithms of pedestrian attribute recognition.
     */
    public enum Algorithm {
        EXT,
        DeepMARCaffeNative,
        //        DeepMARTensorflow,
        Fake
    }


    /**
     * Constructor of the application, configuring properties read from a
     * property center.
     *
     * @param propCenter A class saving all the properties this application may need.
     * @throws Exception Any exception that might occur during execution.
     */
    public PedestrianAttrRecogApp(AppPropertyCenter propCenter) throws Exception {
        super(propCenter, APP_NAME);

        registerStreams(Collections.singletonList(new RecogStream(propCenter)));
    }

    public static class AppPropertyCenter extends SystemPropertyCenter {

        private static final long serialVersionUID = -786439769732467646L;
        public InetAddress externAttrRecogServerAddr = InetAddress.getLocalHost();
        public int externAttrRecogServerPort = 0;
        public Algorithm algorithm = Algorithm.Fake;

        public AppPropertyCenter(@Nonnull String[] args)
                throws URISyntaxException, ParserConfigurationException, SAXException, UnknownHostException {
            super(args);
            // Digest the settings.
            for (Map.Entry<Object, Object> entry : sysProps.entrySet()) {
                switch ((String) entry.getKey()) {
                    case "vpe.ped.attr.ext.ip":
                        externAttrRecogServerAddr = InetAddress.getByName((String) entry.getValue());
                        break;
                    case "vpe.ped.attr.ext.port":
                        externAttrRecogServerPort = Integer.parseInt((String) entry.getValue());
                        break;
                    case "vpe.ped.attr.alg":
                        algorithm = Algorithm.valueOf((String) entry.getValue());
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
     * @throws Exception Any exception that might occur during execution.
     */
    public static void main(String[] args) throws Exception {
        // Load system properties.
        AppPropertyCenter propCenter = new AppPropertyCenter(args);

        // Start the pedestrian tracking application.
        PedestrianAttrRecogApp app = new PedestrianAttrRecogApp(propCenter);
        app.initialize();
        app.start();
        app.awaitTermination();
    }

    public static class RecogStream extends Stream {

        public static final String NAME = "recog";
        public static final DataType OUTPUT_TYPE = DataType.ATTRIBUTES;

        /**
         * Topic to input tracklets from Kafka.
         */
        public static final Port TRACKLET_PORT =
                new Port("pedestrian-tracklet-for-attr-recog", DataType.TRACKLET);
        private static final long serialVersionUID = -4672941060404428484L;

        private Singleton<Recognizer> recognizerSingleton;
        private Singleton<KeyedObjectPool<String,DeepMARCaffeNative>> recognizerSingleton2;
        private Singleton<ObjectPool4DeepMARCaffeNative> recognizerSingleton3;
        
        
        public RecogStream(AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            loggerSingleton.getInst().debug("Using Kafka brokers: " + propCenter.kafkaBootstrapServers);

            switch (propCenter.algorithm) {
                case EXT:
                    recognizerSingleton = new Singleton<>(
                            () -> new ExternRecognizer(
                                    propCenter.externAttrRecogServerAddr,
                                    propCenter.externAttrRecogServerPort,
                                    loggerSingleton.getInst()),
                            ExternRecognizer.class);
                    break;
//                case DeepMARCaffeBytedeco:
//                    recognizerSingleton = new Singleton<>(
//                            () -> new DeepMARCaffeBytedeco(propCenter.caffeGPU, loggerSingleton.getInst()),
//                            DeepMARCaffeBytedeco.class);
//                    break;
//                case DeepMARTensorflow:
//                    recognizerSingleton = new Singleton<>(
//                            () -> new DeepMARTF("", loggerSingleton.getInst()),
//                            DeepMARTF.class);
//                    break;
                case DeepMARCaffeNative:
//                    recognizerSingleton = new Singleton<>(
//                            () -> new DeepMARCaffe2Native(propCenter.caffeGPU, loggerSingleton.getInst()),
//                            DeepMARCaffe2Native.class
//                    );
                  recognizerSingleton3= new Singleton<>(() -> ObjectPool4DeepMARCaffeNative.getInstance(loggerSingleton,reportSingleton.getInst()),ObjectPool4DeepMARCaffeNative.class);
                  recognizerSingleton2 = new Singleton<>(() -> recognizerSingleton3.getInst().pool,KeyedObjectPool.class);
                    break;
                case Fake:
//                    recognizerSingleton = new Singleton<>(
//                            FakeRecognizer::new,
//                            FakeRecognizer.class);
                    break;
                default:
                    throw new NotImplementedException("Attribute recognition algorithm "
                            + propCenter.algorithm + " is not implemented.");
            }
        }

        /**
         * Add streaming actions to the global {@link TaskData} stream.
         * This global stream contains pre-deserialized TaskData messages, so as to save time.
         *
         * @param globalStreamMap A map of streams. The key of an entry is the topic name,
         *                        which must be one of the {@link DataType}.
         *                        The value is a filtered stream.
         */
        @Override
        public void addToGlobalStream(Map<DataType, JavaPairDStream<UUID, TaskData>> globalStreamMap) {
            // Extract tracklets from the data.
            // Recognize attributes from the tracklets.
        	
            this.filter(globalStreamMap, TRACKLET_PORT)
                    .foreachRDD(rdd -> {
                    	JavaRDD<List<Tuple2<UUID, TaskData>>> rddGlom=rdd.glom();
//                    	if (rddGlom.count()>0) {
//						
//                		rddGlom.collect().forEach(list->{
//                			if (list!=null&&list.size()>0) {
//                				System.out.println("属性识别 开始----------------------------");
//								
//                				System.out.println("属性识别list的 大小是："+list.size());
//                				for (int i = 0; i < list.size(); i++) {
//                					TrackletOrURL trackletOrURL = (TrackletOrURL)list.get(i)._2().predecessorRes;
//                					try {
//    									Tracklet tracklet = trackletOrURL.getTracklet();
//    									System.out.println("属性识别要处理的trackletId是:"+tracklet.id);
//    								} catch (Exception e) {
//    									// TODO Auto-generated catch block
//    									e.printStackTrace();
//    								}
//								}
//                				
//                				System.out.println("需要处理的数据的个数是："+rddGlom.count());
//							}
//                			});
//					}
                            	
                  rddGlom.foreachPartition(iterator->{
        			while(iterator.hasNext()){
        				List<Tuple2<UUID, TaskData>> kvList=iterator.next();
//        				long id=Thread.currentThread().getId();
                        Logger logger = loggerSingleton.getInst();
                        long startTime = System.currentTimeMillis();
                        final long[] recognizerCostTime = {0};
                        final int[] numSamples = {0};
                        if (kvList.size()>0) {
						
							logger.info("属性识别开始--------------:"+startTime);
//							logger.info("线程："+id+"，属性识别开始--------------:"+startTime);
							
							MysqlDaoJdbc mysqlDaoJdbc = mysqlSingleton.getInst();
							List<DataInfo> dataInfoList=new ArrayList<>();
							
                        	ObjectPool4DeepMARCaffeNative objectPool4DeepMARCaffeNative=recognizerSingleton3.getInst();
                        	KeyedObjectPool<String, DeepMARCaffeNative> pool=recognizerSingleton2.getInst();
                        	ThreadLocal<Integer> tl = new ThreadLocal<Integer>();
                        	
//                        	List<Integer> gpus=objectPool.gpus;
//                    		System.out.println("PedestrianAttrRecogApp里的gpus是："+gpus);
                        	
                        	ConcurrentHashMap<Integer,Integer> map=objectPool4DeepMARCaffeNative.map;
                        	
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
									logger.info("key是空的");
								}
							}else {
								pickGpu=tl.get();
							}
                        	
                        	String key =String.valueOf(pickGpu) ;
//                    			System.out.println("PedestrianAttrRecogApp使用的key是："+key);
                        	
							long recogStartTime = System.currentTimeMillis();
                        	final DeepMARCaffeNative deepMARCaffeNative =objectPool4DeepMARCaffeNative.getDeepMARCaffeNative(key, pool);
                        	
//							final DeepMARCaffe2Native deepMARCaffe2Native=(DeepMARCaffe2Native) recognizerSingleton.getInst();
                        	
                            /*boolean flag = true;
                            try {
//                            deepMARCaffe2Native=(DeepMARCaffe2Native) recognizerSingleton.getInst();
                            	
//                            pool=recognizerSingleton2.getInst();
//                        	DeepMARCaffe2Native deepMARCaffe2Native = pool.borrowObject(key);
                            	deepMARCaffe2Native =objectPool.getDeepMARCaffe2Native(key, pool);
//                            	//当前池里的实例数量  
//						        System.out.println("当前线程id："+tid+",刚借出后，池中所有在用实例pool.getNumActive()："+recognizerSingleton2.getInst().getNumActive());  
//						        System.out.println("当前线程id："+tid+",刚借出后，池中所有在用实例pool.getNumActive("+key+")："+recognizerSingleton2.getInst().getNumActive(key));  
//						        //当前池里的处于闲置状态的实例  
//						        System.out.println("当前线程id："+tid+",刚借出后，池中处于闲置状态的实例pool.getNumIdle()："+recognizerSingleton2.getInst().getNumIdle());  
//						        System.out.println("当前线程id："+tid+",刚借出后，池中处于闲置状态的实例pool.getNumIdle("+key+")："+recognizerSingleton2.getInst().getNumIdle(key));    
							} catch (Exception e) {
								// TODO: handle exception
								flag = false;  
					            e.printStackTrace();
							}*/
                            long borrowObjectTime=System.currentTimeMillis()-recogStartTime;
                            logger.info("PedestrianAttrRecogApp使用的gpu是:"+ deepMARCaffeNative.gpu);
                            
                            
                     kvList.forEach(kv -> {
                        if(kv!=null){
                        	long startEachTime = System.currentTimeMillis();
                        	try {
                        		
                    			 
                        		try {
                            	

                                final UUID taskID = kv._1();
                                final TaskData taskData = kv._2();
                                logger.debug("To recognize attributes for task " + taskID + "!");
                                
                                DataInfo dataInfo=new DataInfo();
                                dataInfo.setStartEachTime(startEachTime);
                                dataInfo.setType("attr");
                                dataInfo.setIsMultiGpu("Multi");
//                                dataInfo.setIsMultiGpu("single");
                                
                                dataInfo.setTaskID(taskID.toString());
                                String nodeName=getServerName();
        						dataInfo.setNodeName(nodeName);
        						dataInfo.setBorrowObjectTime(borrowObjectTime);
                                dataInfo.setGpu(deepMARCaffeNative.gpu);
                                dataInfo.setNodeName_gpu(nodeName+"-"+deepMARCaffeNative.gpu);
                                dataInfo.setToString(deepMARCaffeNative.toString());
                                dataInfo.setHashCode(System.identityHashCode(deepMARCaffeNative));
                                
                                // Recognize attributes robustly.
                                if (taskID!=null&&taskData!=null) {
                                	TrackletOrURL trackletOrURL=(TrackletOrURL) taskData.predecessorRes;
                                	Tracklet tracklet =null;
                                	if (trackletOrURL!=null) {
									try {
										
										tracklet = trackletOrURL.getTracklet();
									} catch (Exception e) {
										// TODO: handle exception
										e.printStackTrace();
									}
                                	dataInfo.setTrackletId(tracklet.id.toString());;
                                	dataInfo.setVideoName(tracklet.id.videoID);
                                	logger.debug("To extract reid feature for tracklet.id " + tracklet.id.toString() + "!");
                                	RobustExecutor<TrackletOrURL, Attributes> robustExecutor=new RobustExecutor<>(
                                			(Function<TrackletOrURL, Attributes>) tou -> {
                                				Attributes a =null;
                                				if (tou!=null&&(tou.getTracklet() != null || tou.getURL() != null)) {
			                                		final Tracklet t = tou.getTracklet();
			                                            try {
															
			                                            	if (t!=null&&t.locationSequence!=null&&t.locationSequence.length>0) {
			                                            	  a = deepMARCaffeNative.recognize(t);
			                                            
			                                            long recogEndTime = System.currentTimeMillis();
			                                            recognizerCostTime[0] += recogEndTime - recogStartTime;
			                                            	Collection<BoundingBox> list=t.getSamples();
															numSamples[0] += list.size();
			                                            }else {
															logger.debug("Tracklet为空,或者locationSequence为空!");
														}
			                                            try {
															
			                                            	a.trackletID = t.id;
														} catch (Exception e) {
															// TODO: handle exception
															e.printStackTrace();
														}
			                                            } catch (Exception e) {
			                                            	// TODO: handle exception
//			                                            	dataInfo.setIsException(1);
			                                            	e.printStackTrace();
			                                            }
                                				} 
//                                            if (a!=null) {
												
                                            	return a; 
//                                        	}else {
//        										return null;
//        									}
                                        });
                                	Attributes attr =null;
                                	if (robustExecutor!=null) {
										
                                		attr = (Attributes) robustExecutor.execute((TrackletOrURL) trackletOrURL);
									}
                                logger.debug("Attributes retrieved for task " + taskID + "!");
                                long endEachTime=System.currentTimeMillis();
                                dataInfo.setEndEachTime(endEachTime);
//                                if (dataInfo.getIsException()==0) {
									dataInfoList.add(dataInfo);
//								}
                                // Find current node.
                                final TaskData.ExecutionPlan.Node curNode = taskData.getDestNode(TRACKLET_PORT);
                                // Get ports to output to.
                                assert curNode != null;
                                final List<TaskData.ExecutionPlan.Node.Port> outputPorts = curNode.getOutputPorts();
                                // Mark the current node as executed.
                                curNode.markExecuted();
                                if (attr!=null&&outputPorts!=null&&taskData.executionPlan!=null&&taskData.userPlan!=null&&taskID!=null) {
									
                                	output(outputPorts, taskData.executionPlan, attr, taskData.userPlan, taskID);
                                }
                                }
                                }
	                            } catch (Exception e) {
	//                                logger.error("During processing attributes.", e);
	                                e.printStackTrace();
	                            }
							} catch (Exception e) {
											// TODO: handle exception
								e.printStackTrace();
							}
						}
				});
                     try {  
//							if (flag) {  
								objectPool4DeepMARCaffeNative.releaseDeepMARCaffeNative(key, deepMARCaffeNative, pool);
//								//当前池里的实例数量  
//					            System.out.println("当前线程id："+tid+",release后，池中所有在用实例pool.getNumActive()："+recognizerSingleton2.getInst().getNumActive());  
//					            System.out.println("当前线程id："+tid+",release后，池中所有在用实例pool.getNumActive("+key+")："+recognizerSingleton2.getInst().getNumActive(key));  
//					            //当前池里的处于闲置状态的实例  
//					            System.out.println("当前线程id："+tid+",release后，池中处于闲置状态的实例pool.getNumIdle()："+recognizerSingleton2.getInst().getNumIdle());  
//					            System.out.println("当前线程id："+tid+",release后，池中处于闲置状态的实例pool.getNumIdle("+key+")："+recognizerSingleton2.getInst().getNumIdle(key));  
//							} else {  
//								objectPool.invalidateDeepMARCaffe2Native(key, deepMARCaffe2Native, pool);
//								//当前池里的实例数量  
//					            System.out.println("当前线程id："+tid+",invalidate后，池中所有在用实例pool.getNumActive()："+recognizerSingleton2.getInst().getNumActive());  
//					            System.out.println("当前线程id："+tid+",invalidate后，池中所有在用实例pool.getNumActive("+key+")："+recognizerSingleton2.getInst().getNumActive(key));  
//					            //当前池里的处于闲置状态的实例  
//					            System.out.println("当前线程id："+tid+",invalidate后，池中处于闲置状态的实例pool.getNumIdle()："+recognizerSingleton2.getInst().getNumIdle());  
//					            System.out.println("当前线程id："+tid+",invalidate后，池中处于闲置状态的实例pool.getNumIdle("+key+")："+recognizerSingleton2.getInst().getNumIdle(key)); 
//							}  
						} catch (Exception e) {  
							e.printStackTrace();  
						}finally {
//							objectPool4Reid.updateMap(map,pickGpu);
							map.replace(pickGpu,1,0);
						}
                     		mysqlDaoJdbc.addAttrInfo(dataInfoList);
					}
                    if (kvList.size() > 0) {
						long endTime = System.currentTimeMillis();
						logger.info("Overall speed=" + ((endTime - startTime) / kvList.size())
										+ "ms per tracklet (totally " + kvList.size() + " tracklets)");
					}
					if (numSamples[0] > 0) {
						logger.info("Recognizer speed=" + (recognizerCostTime[0] / numSamples[0])
								+ "ms per sample (totally " + numSamples[0] + " samples)");
					}
                  }});

			});

		}

        /**
         * Get input ports of the stream.
         *
         * @return A list of ports.
         */
        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(TRACKLET_PORT);
        }

    }
}
