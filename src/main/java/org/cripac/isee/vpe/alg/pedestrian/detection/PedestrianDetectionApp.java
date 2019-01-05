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

package org.cripac.isee.vpe.alg.pedestrian.detection;

import static org.cripac.isee.util.ResourceManager.getResource;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.KafkaException;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.cripac.isee.alg.pedestrian.detection.Detector;
import org.cripac.isee.alg.pedestrian.detection.ObjectDetection;
import org.cripac.isee.alg.pedestrian.tracking.BasicTracker;
import org.cripac.isee.alg.pedestrian.tracking.SSDTracker;
import org.cripac.isee.alg.pedestrian.tracking.Tracker;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.util.Singleton;
import org.cripac.isee.vpe.alg.pedestrian.tracking.TrackletOrURL;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.ParallelExecutor;
import org.cripac.isee.vpe.common.RobustExecutor;
import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan;
import org.cripac.isee.vpe.data.MysqlDaoJdbc;
import org.cripac.isee.vpe.entities.DataInfo;
import org.cripac.isee.vpe.util.hdfs.HDFSFactory;
import org.cripac.isee.vpe.util.hdfs.HadoopHelper;
import org.cripac.isee.vpe.util.logging.Logger;
import org.xml.sax.SAXException;

import kafka.common.FailedToSendMessageException;
import kafka.common.MessageSizeTooLargeException;
import scala.Tuple2;

/**
 * The PedestrianReIDFeatureExtractionApp class is a Spark Streaming application
 * which performs reid feature extraction.
 *
 * @author da.li, CRIPAC, 2017
 */
public class PedestrianDetectionApp extends SparkStreamingApp {
    /**
	 * 
	 */
	private static final long serialVersionUID = 5694451779683940546L;
	/**
     * The name of this application.
     */
    public static final String APP_NAME = "pedestrian-detection";

    @Override
    public void addToContext() throws Exception {
        // Do nothing.
    }


    /**
     * Constructor of the application, configuring properties read from a
     * property center.
     *
     * @param propCenter A class saving all the properties this application may need.
     * @throws Exception On failure in Spark.
     */
    public PedestrianDetectionApp(AppPropertyCenter propCenter) throws Exception {
        super(propCenter, APP_NAME);

        registerStreams(Collections.singletonList(new PedestrianDetectionStream(propCenter)));
    }

    public static class AppPropertyCenter extends SystemPropertyCenter {

		private static final long serialVersionUID = 5291846696973850058L;
		int numSamplesPerTracklet = -1;
        // Add by da.li on 2017/06/06 for ssd tracker:
        public int trackerMethod = 0; 
        // public int gpuID = -1;
        public float confidenceThreshold = 0.5f;
        // End

        public AppPropertyCenter(@Nonnull String[] args)
                throws SAXException, ParserConfigurationException, URISyntaxException {
            super(args);
            // Digest the settings.
            for (Map.Entry<Object, Object> entry : sysProps.entrySet()) {
                switch ((String) entry.getKey()) {
                    case "vpe.num.sample.per.tracklet":
                        numSamplesPerTracklet = Integer.valueOf((String) entry.getValue());
                        break;
                    case "tracker.method":
                        trackerMethod = Integer.valueOf((String) entry.getValue());
                        break;
                    //case "tracker.gpuID":
                    //    gpuID = Integer.valueOf((String) entry.getValue());
                    //    break;
                    case "driver.memory":
                        driverMem = (String) entry.getValue();
                        break;
                    case "executor.memory":
                        executorMem = (String) entry.getValue();
                        break;
                    case "executor.cores":
  						executorCores = Integer.parseInt((String) entry.getValue());
  						
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
        SparkStreamingApp app = new PedestrianDetectionApp(propCenter);
        app.initialize();
        app.start();
        app.awaitTermination();
    }

    public static class PedestrianDetectionStream extends Stream {

        /**
		 * 
		 */
		private static final long serialVersionUID = 5207533980885737969L;
		public static final String NAME = "hdfs-video-detection";
        public static final DataType OUTPUT_TYPE = DataType.DETECTION;

        /**
         * Port to input video URLs from Kafka.
         */
        public static final Port VIDEO_URL_PORT =
                new Port("hdfs-video-url-for-pedestrian-detection", DataType.URL);

        private class ConfCache extends HashMap<String, byte[]> {

			private static final long serialVersionUID = 9098522538646064414L;
        }

        private final Singleton<ConfCache> confCacheSingleton;
        private final int numSamplesPerTracklet;
        private final String metadataDir;

        // Add by da.li on 2017/06/06
        // private final int gpuID;
        private final String caffeGPU;
        private final int trackerMethod;
        private final float confidenceThreshold;
        //private final File pbFile;
        //private final File modelFile;
        // End

        public PedestrianDetectionStream(AppPropertyCenter propCenter) throws Exception {
        	 super(APP_NAME, propCenter);

             numSamplesPerTracklet = propCenter.numSamplesPerTracklet;
             metadataDir = propCenter.metadataDir;
             confCacheSingleton = new Singleton<>(ConfCache::new, ConfCache.class);

             // Add by da.li on 2017/06/06
             // gpuID = propCenter.gpuID;
             caffeGPU = propCenter.caffeGPU;
             trackerMethod = propCenter.trackerMethod;
             confidenceThreshold = propCenter.confidenceThreshold;
             //pbFile = getResource("/models/SSDCaffe/deploy.prototxt");
             //modelFile = getResource("/models/SSDCaffe/deploy.caffemodel");
             // End
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
        	//得到的是AM Container的pid
//        	int pid=WebToolUtils.getPID();
//        	System.out.println("PedestrianTrackingApp 的addToGlobalStream pid："+pid);
//        	JavaPairDStream<UUID, TaskData> globalStreamFilter=this.filter(globalStreamMap, VIDEO_URL_PORT);
//        	System.out.println("globalStreamFilter.count是:"+globalStreamFilter.count());
//        	System.out.println("globalStreamFilter.countByValue是："+globalStreamFilter.countByValue());
            this.filter(globalStreamMap, VIDEO_URL_PORT)
                    .foreachRDD(rdd -> {
                    	JavaRDD<List<Tuple2<UUID, TaskData>>> rddGlom=rdd.glom();
                    	//rddGlom一直会传过来，但是里边没有数据了
                    	/*if (rddGlom.count()>0) {
							
                    		System.out.println("tracking 开始----------------------------");
//                    		rddGlom.collect().forEach(list->System.out.println("需要处理的视频:"+list));;
                    		System.out.println("需要处理的视频的个数是："+rddGlom.count());
						}*/
                    	rddGlom.foreach(kvList -> {
                    		final Logger logger = loggerSingleton.getInst();
                    		if (kvList.size() > 0) {
                            logger.info("detection Partition " + TaskContext.getPartitionId()
                                    + " got " + kvList.size()
                                    + " videos in this batch.");
//                        }

                            List<DataInfo> daInfoList=new ArrayList<>();
                        long startTime = System.currentTimeMillis();
                        synchronized (PedestrianDetectionStream.class) {
                        	kvList.forEach(kv->{
//                            ParallelExecutor.execute(kvList, kv -> {
                            	final FileSystem hdfs = HDFSFactory.newInstance();
                                try {
                                	long startEachTime = System.currentTimeMillis();
                                    final UUID taskID = kv._1();
                                    final TaskData taskData = kv._2();
                                    
                                    DataInfo dataInfo=new DataInfo();
//                                    dataInfo.setStartTime(startTime);
                                    dataInfo.setType("detetion");
                                    dataInfo.setTaskID(taskID.toString());
                                    dataInfo.setStartEachTime(startEachTime);
                                    String nodeName=getServerName();
                                    dataInfo.setNodeName(nodeName);
                                    
                                    final String videoURL = (String) taskData.predecessorRes;
                                    logger.debug("detection Received taskID=" + taskID + ", URL=" + videoURL);

                                    final Path videoPath = new Path(videoURL);
                                    String videoName = videoPath.getName();
                                    videoName = videoName.substring(0, videoName.lastIndexOf('.'));
                                    
                                    dataInfo.setVideoName(videoName);
                                    
                                    // Find current node.
                                    final ExecutionPlan.Node curNode = taskData.getDestNode(VIDEO_URL_PORT);
                                    // Get tracking configuration for this execution.
                                    assert curNode != null;
                                    final String confFile = (String) curNode.getExecData();
                                    if (confFile == null) {
                                        throw new IllegalArgumentException(
                                                "detetion configuration file is not specified for this node!");
                                    }

                                    // Get ports to output to.
                                    final List<ExecutionPlan.Node.Port> outputPorts = curNode.getOutputPorts();
                                    // Mark the current node as executed in advance.
                                    curNode.markExecuted();

                                    // Load tracking configuration to create a tracker.
                                    if (!confCacheSingleton.getInst().containsKey(confFile)) {
                                        InputStream confStream = getClass().getResourceAsStream(
                                                "/conf/" + APP_NAME + "/" + confFile);
                                        if (confStream == null) {
                                            throw new IllegalArgumentException(
                                                    "detection  configuration file not found in JAR!");
                                        }
                                        confCacheSingleton.getInst().put(confFile, IOUtils.toByteArray(confStream));
                                    }
                                    final byte[] confBytes = confCacheSingleton.getInst().get(confFile);
                                    if (confBytes == null) {
                                        logger.fatal("confPool contains key " + confFile + " but value is null!");
                                        return;
                                    }

                                    File deteFile=getResource("/models/Detection/ssd300_mAP_77.43_v2.pth");
                                    Detector detector = new ObjectDetection(-1,deteFile, logger);
                                    
//                                    // Modified by da.li on 2017/06/06
//                                    final Tracker tracker;
//                                    File pbFile = null;
//                                    File modelFile = null;
//                                    if (trackerMethod == 0) { // Motion Detection.
//                                        logger.debug("Now using BASIC Tracker!");
//                                        tracker = new BasicTracker(confBytes, logger);
//                                    } else if (trackerMethod == 1) { // SSD
//                                        pbFile = getResource("/models/SSDCaffe/deploy.prototxt");
//                                        modelFile = getResource("/models/SSDCaffe/deploy.caffemodel");
//                                        
//                                        dataInfo.setGpu(caffeGPU);
//                                        dataInfo.setNodeName_gpu(nodeName+"-"+caffeGPU);
//                                        
//                                        tracker = new SSDTracker(confBytes,
//                                                                 caffeGPU,
//                                                                 confidenceThreshold,
//                                                                 pbFile,
//                                                                 modelFile,
//                                                                 logger);
//                                        
//                                    } else {
//                                        logger.fatal("BAD tracking method!");
//                                        return;
//                                    }
                                    // End
                                    
                                    //final Tracker tracker = new BasicTracker(confBytes, logger);

                                   

                                    // Conduct tracking on video read from HDFS.
                                    logger.debug("Performing detection  on " + videoName);
                                    new RobustExecutor<Void, Void>(() ->{
                                    final int BUFFER_SIZE = 4096 * 2160 * 3;
                                    InputStream videoStream =
                                           new BufferedInputStream(hdfs.open(videoPath), BUFFER_SIZE);
		                                    try {
		                                    	detector.detection(videoStream);
												
											} catch (Exception e) {
												// TODO: handle exception
												e.printStackTrace();
											}finally {
												
												try {
													if (videoStream!=null) {
														videoStream.close();
														videoStream=null;
													}
													if (detector!=null) {
														
														detector.free();
													}
												} catch (Exception e1) {
													// TODO: handle exception
													e1.printStackTrace();
												}
											}
                                    }).execute();
                                    
//                                    final Tracklet[] tracklets = new RobustExecutor<Void, Tracklet[]>(
//                                            (Function0<Tracklet[]>) () -> {
//                                                // This value is set according to resolution of DCI 4K.
//                                                final int BUFFER_SIZE = 4096 * 2160 * 3;
//                                                 InputStream videoStream =
//                                                        new BufferedInputStream(hdfs.open(videoPath), BUFFER_SIZE);
//                                                Tracklet[] trackletsArr =tracker.track(videoStream);
//                                                try {
//													if (videoStream!=null) {
//														videoStream.close();
//														videoStream=null;
//													}
//												} catch (Exception e) {
//													// TODO: handle exception
//													e.printStackTrace();
//												}
//                                                return trackletsArr;
//                                            }
//                                    ).execute();
                                    
                                    logger.debug("Finished detection  on " + videoName);
                                    
                                    long endEachTime=System.currentTimeMillis();
                                    dataInfo.setEndEachTime(endEachTime);
                                    // New add on 2017/0629
//                                    if (trackerMethod == 1) {
//                                        pbFile.delete();
//                                        modelFile.delete();
//                                        logger.debug("Tmp file deleted!");
//                                    } 
                                    // End by da.li
                                    // Set video IDs and Send tracklets.
//                                    System.out.println("视频"+videoName+"发送的tracklets的大小是："+tracklets.length);
//                                    dataInfo.setTrackletSize(tracklets.length);
                                    
//                                    for (Tracklet tracklet : tracklets) {
//                                        // Conduct sampling on the tracklets to save memory.
//                                        tracklet.sample(numSamplesPerTracklet);
//                                        tracklet.id.videoID = videoName;
//                                        dataInfo.setTrackletId(tracklet.id.toString());
//                                        daInfoList.add(dataInfo);
//                                        
//                                        MysqlDaoJdbc mysqlDaoJdbc=mysqlSingleton.getInst();
//                                        mysqlDaoJdbc.addTrackingInfo(dataInfo);
//                                        
//                                        final String videoRoot = metadataDir + "result/"+"motion/" +dateString+"/" + tracklet.id.videoID;
//                                        final String taskRoot = videoRoot + "/" + taskID;
//                                        final String storeDir = taskRoot + "/" + tracklet.id.serialNumber;
//                                        try {
//                                            output(outputPorts, 
//                                                   taskData.executionPlan,
//                                                   new TrackletOrURL(tracklet), 
//                                                   taskData.userPlan,
//                                                   taskID);
//                                            //为了单独测试属性识别和reid，不加datamanaging，在这里进行保存
////                                            HadoopHelper.storeTrackletNew(storeDir, tracklet, hdfs); 
////                                            System.out.println("发送的tracklet："+tracklet.id);
//                                        } catch (MessageSizeTooLargeException
//                                                | KafkaException
//                                                | FailedToSendMessageException e) {
//                                            // The tracklet's size exceeds the limit.
//                                            // Here we first store it into HDFS,
//                                            // then send its URL instead of the tracklet itself.
//                                            
//                                            logger.debug("detection  Tracklet " + tracklet.id
//                                                    + " is too long. Passing it through HDFS at \"" + storeDir + "\".");
//                                            HadoopHelper.storeTrackletNew(storeDir, tracklet, hdfs); // Add new ... by da.li
//                                            output(outputPorts,
//                                                   taskData.executionPlan,
//                                                   new TrackletOrURL(storeDir),
//                                                   taskData.userPlan,
//                                                   taskID);
////                                            System.out.println("tracklet太长，发送的tracklet："+tracklet.id);
//                                        }
//                                    }

//                                    hdfs.close();
                                } catch (Throwable e) {
//                                    logger.error("During tracking.", e);
                                	System.out.println("在detection 时发生异常");
                                    e.printStackTrace();
                                }finally {
                                	try {
                                		if (hdfs!=null) {
                                			hdfs.close();
                                		}
										
									} catch (Exception e) {
										// TODO: handle exception
										e.printStackTrace();
									}
								}
                            });
                            /*
                            if (pbFile.delete()) {
                                logger.debug("Tmp pb file has been deleted!");
                            } else {
                                logger.fatal("An ERROR in deleting pb file.");
                            }
                            if (modelFile.delete()) {
                                logger.debug("Tmp model file has been deleted!");
                            } else {
                                logger.fatal("An ERROR in deleting model file.");
                            }
                            */ 
                        }
//                        if (kvList.size() > 0) {
                            long endTime = System.currentTimeMillis();
                            logger.info(kvList.size()+"个视频,Average cost time: " + ((endTime - startTime) / kvList.size()) + "ms");
                            
                            System.out.println("daInfoList的大小是："+daInfoList.size());
                        }
                    });
                    	
                    	/*if (rddGlom.count()>0) {
							
                    		System.out.println("tracking 结束----------------------------");
                    		System.out.println("视频的个数是："+rddGlom.count());
						}*/
                    });
//            if (globalStreamMap.size()>0) {
//				
//			}
            
        }

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(VIDEO_URL_PORT);
        }

    }
}
