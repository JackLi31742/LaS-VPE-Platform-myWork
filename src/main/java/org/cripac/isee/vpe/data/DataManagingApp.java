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

package org.cripac.isee.vpe.data;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameGrabber;
import org.cripac.isee.alg.pedestrian.attr.Attributes;
import org.cripac.isee.alg.pedestrian.reid.Feature;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.util.SerializationHelper;
import org.cripac.isee.util.Singleton;
import org.cripac.isee.util.ftp.Ftp;
import org.cripac.isee.util.ftp.FtpUtil;
import org.cripac.isee.vpe.alg.pedestrian.tracking.TrackletOrURL;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.ParallelExecutor;
import org.cripac.isee.vpe.common.ParallelExecutor2;
import org.cripac.isee.vpe.common.RobustExecutor;
import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan;
import org.cripac.isee.vpe.entities.Data4Neo4j;
import org.cripac.isee.vpe.util.hdfs.HDFSFactory;
import org.cripac.isee.vpe.util.hdfs.HadoopHelper;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.xml.sax.SAXException;

import scala.Tuple2;

/**
 * The DataManagingApp class combines two functions: meta data saving and data
 * feeding. The meta data saving function saves meta data, which may be the
 * results of vision algorithms, to HDFS and Neo4j database. The data feeding
 * function retrieves stored results and sendWithLog them to algorithm modules from
 * HDFS and Neo4j database. The reason why combine these two functions is that
 * they should both be modified when and only when a new data inputType shall be
 * supported by the system, and they require less resources than other modules,
 * so combining them can save resources while not harming performance.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class DataManagingApp extends SparkStreamingApp {
    /**
     * The name of this application.
     */
    public static final String APP_NAME = "data-managing";
    private static final long serialVersionUID = 7338424132131492017L;

    public DataManagingApp(AppPropertyCenter propCenter) throws Exception {
        super(propCenter, APP_NAME);
        kafkaParams.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        kafkaParams.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 2*ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES);
        kafkaParams.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 128*1024);
        kafkaParams.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 2*ConsumerConfig.DEFAULT_FETCH_MAX_BYTES);
        kafkaParams.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 10);
        registerStreams(Arrays.asList(
                new TrackletSavingStream(propCenter),
                new AttrSavingStream(propCenter),
//                new IDRankSavingStream(propCenter),
//                new VideoCuttingStream(propCenter),
                new ReidFeatureSavingStream(propCenter),
                new PedestrianDetectionSavingStream(propCenter)
        		));
    }

    public static class AppPropertyCenter extends SystemPropertyCenter {

        private static final long serialVersionUID = -786439769732467646L;

        int maxFramePerFragment = 1000;

        public AppPropertyCenter(@Nonnull String[] args)
                throws URISyntaxException, ParserConfigurationException, SAXException, UnknownHostException {
            super(args);
            // Digest the settings.
            for (Map.Entry<Object, Object> entry : sysProps.entrySet()) {
                switch ((String) entry.getKey()) {
                    case "vpe.max.frame.per.fragment":
                        maxFramePerFragment = Integer.parseInt((String) entry.getValue());
                        break;
                    case "kafka.partitions":
                        kafkaNumPartitions = Integer.parseInt((String) entry.getValue());
                        break;
                    default:
                        logger.warn("Unrecognized option: " + entry.getKey());
                        break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final AppPropertyCenter propCenter = new AppPropertyCenter(args);

        AtomicReference<Boolean> running = new AtomicReference<>();
        running.set(true);

        //Thread packingThread = new Thread(new TrackletPackingThread(propCenter, running));
        //packingThread.start();

        final SparkStreamingApp app = new DataManagingApp(propCenter);
        app.initialize();
        app.start();
        app.awaitTermination();
        running.set(false);
    }

    public static class VideoCuttingStream extends Stream {

        public final static Port VIDEO_URL_PORT = new Port("video-url-for-cutting", DataType.URL);
        private static final long serialVersionUID = -6187153660239066646L;
        public static final DataType OUTPUT_TYPE = DataType.FRAME_ARRAY;

        int maxFramePerFragment;

        /**
         * Initialize necessary components of a Stream object.
         *
         * @param propCenter System property center.
         * @throws Exception On failure creating singleton.
         */
        public VideoCuttingStream(AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);
            maxFramePerFragment = propCenter.maxFramePerFragment;
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
            this.filter(globalStreamMap, VIDEO_URL_PORT)
                    .foreachRDD(rdd -> rdd.foreachPartition(kvIter -> {
                        synchronized (VideoCuttingStream.class) {
                            final Logger logger = loggerSingleton.getInst();
                            ParallelExecutor.execute(kvIter, kv -> {
                                try {
                                    new RobustExecutor<Void, Void>(() -> {
                                        final UUID taskID = kv._1();
                                        final TaskData taskData = kv._2();

                                        final FileSystem hdfs = HDFSFactory.newInstance();
                                        FFmpegFrameGrabber frameGrabber = new FFmpegFrameGrabber(
                                                hdfs.open(new Path((String) taskData.predecessorRes))
                                        );

                                        Frame[] fragments = new Frame[maxFramePerFragment];
                                        int cnt = 0;
                                        final ExecutionPlan.Node curNode = taskData.getDestNode(VIDEO_URL_PORT);
                                        assert curNode != null;
                                        final List<ExecutionPlan.Node.Port> outputPorts = curNode.getOutputPorts();
                                        curNode.markExecuted();
                                        while (true) {
                                            Frame frame;
                                            try {
                                                frame = frameGrabber.grabImage();
                                            } catch (FrameGrabber.Exception e) {
                                                logger.error("On grabImage: " + e);
                                                if (cnt > 0) {
                                                    Frame[] lastFragments = new Frame[cnt];
                                                    System.arraycopy(fragments, 0, lastFragments, 0, cnt);
                                                    // output(outputPorts, taskData.executionPlan, lastFragments, taskID);
                                                    output(outputPorts, taskData.executionPlan, lastFragments, null, taskID); // modified dli
                                                }
                                                break;
                                            }
                                            if (frame == null) {
                                                if (cnt > 0) {
                                                    Frame[] lastFragments = new Frame[cnt];
                                                    System.arraycopy(fragments, 0, lastFragments, 0, cnt);
                                                    //output(outputPorts, taskData.executionPlan, lastFragments, taskID);
                                                    output(outputPorts, taskData.executionPlan, lastFragments, null, taskID); // modified dli
                                                }
                                                break;
                                            }

                                            fragments[cnt++] = frame;
                                            if (cnt >= maxFramePerFragment) {
                                                //output(outputPorts, taskData.executionPlan, fragments, taskID);
                                                output(outputPorts, taskData.executionPlan, fragments, null, taskID); // modified dli
                                                cnt = 0;
                                            }
                                        }
                                    }).execute();
                                } catch (Throwable t) {
                                    logger.error("On cutting video", t);
                                }
                            });
                        }
                    }));
        }

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(VIDEO_URL_PORT);
        }
    }

    /**
     * This is a thread independent from Spark Streaming,
     * which listen to tracklet packing jobs from Kafka,
     * and perform HAR packing. There is no need to worry
     * about job loss due to system faults, since offsets
     * are committed after jobs are finished, so interrupted
     * jobs can be retrieved from Kafka and executed again
     * on another start of this thread. This thread is to be
     * started together with the DataManagingApp.
     */
    static class TrackletPackingThread implements Runnable {

        final static String JOB_TOPIC = "tracklet-packing-job";

        final Properties consumerProperties;
        final String metadataDir;
        final Logger logger;
        private final AtomicReference<Boolean> running;
        final GraphDatabaseConnector dbConnector;
        private final static int MAX_POLL_INTERVAL_MS = 300000;
        private int maxPollRecords = 500;

        TrackletPackingThread(AppPropertyCenter propCenter, AtomicReference<Boolean> running) {
            consumerProperties = propCenter.getKafkaConsumerProp("tracklet-packing", false);
            consumerProperties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "" + maxPollRecords);
            consumerProperties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "" + MAX_POLL_INTERVAL_MS);
            metadataDir = propCenter.metadataDir;
            logger = new SynthesizedLogger(APP_NAME, propCenter);
            this.running = running;
            // dbConnector = new FakeDatabaseConnector();
            dbConnector = new Neo4jConnector();
        }

        @Override
        public void run() {
            while (running.get()) {
                try {
                    KafkaConsumer<String, byte[]> jobListener = new KafkaConsumer<>(consumerProperties);
                    final FileSystem hdfs;
                    FileSystem tmpHDFS;
                    while (true) {
                        try {
                            tmpHDFS = new HDFSFactory().produce();
                            break;
                        } catch (IOException e) {
                            logger.error("On connecting HDFS", e);
                        }
                    }
                    hdfs = tmpHDFS;
                    jobListener.subscribe(Collections.singletonList(JOB_TOPIC));
                    while (running.get()) {
                        ConsumerRecords<String, byte[]> records = jobListener.poll(1000);
                        Map<String, byte[]> taskMap = new HashMap<>();
                        records.forEach(rec -> taskMap.put(rec.key(), rec.value()));

                        final long start = System.currentTimeMillis();
                        logger.info("Packing thread received " + taskMap.keySet().size() + " jobs.");
                        //TODO(Ken Yu): Make sure whether executing HAR packing in parallel is faster.
                        //TODO(Ken Yu): Find the best parallelism.
                        ParallelExecutor.execute(taskMap.entrySet(), 4, kv -> {
                            try {
                                final String taskID = kv.getKey();
                                final byte[] value = kv.getValue();
                                //final Tuple2<String, Integer> info = SerializationHelper.deserialize(value);
                                //final String videoID = info._1();
                                // Modified "String" in Tuple2 to "Tracklet.Identifier" on 2017/04/26
                                final Tuple2<Tracklet.Identifier, Integer> info = SerializationHelper.deserialize(value);
                                final Tracklet.Identifier trackletID = info._1();
                                final String videoID = trackletID.videoID;
                                final int numTracklets = info._2();
                                final String videoRoot = metadataDir + "result/" + videoID;
                                final String taskRoot = videoRoot + "/" + taskID;

                                final boolean harExists = new RobustExecutor<Void, Boolean>(
                                        (Function0<Boolean>) () ->
                                                hdfs.exists(new Path(videoRoot + "/" + taskID + ".har"))
                                ).execute();
                                if (harExists) {
                                    // Packing has been finished in a previous request.
                                    final boolean taskRootExists = new RobustExecutor<Void, Boolean>(
                                            (Function0<Boolean>) () ->
                                                    hdfs.exists(new Path(videoRoot + "/" + taskID))
                                    ).execute();
                                    if (taskRootExists) {
                                        // But seems to have failed to delete the task root.
                                        // Now do it again.
                                        new RobustExecutor<Void, Void>(() ->
                                                hdfs.delete(new Path(taskRoot), true)
                                        ).execute();
                                    }
                                    return;
                                }

                                // If all the tracklets from a task are saved,
                                // it's time to pack them into a HAR!
                                final ContentSummary contentSummary = new RobustExecutor<Void, ContentSummary>(
                                        (Function0<ContentSummary>) () -> hdfs.getContentSummary(new Path(taskRoot))
                                ).execute();
                                final long dirCnt = contentSummary.getDirectoryCount();
                                // Decrease one for directory counter.
                                if (dirCnt - 1 == numTracklets) {
                                    logger.info("Starting to pack tracklets for task " + taskID
                                            + "(" + videoID + ")! The directory consumes "
                                            + contentSummary.getSpaceConsumed() + " bytes.");

                                    new RobustExecutor<Void, Void>(() -> {
                                        final HadoopArchives arch = new HadoopArchives(HadoopHelper.getDefaultConf());
                                        final ArrayList<String> harPackingOptions = new ArrayList<>();
                                        harPackingOptions.add("-archiveName");
                                        harPackingOptions.add(taskID + ".har");
                                        harPackingOptions.add("-p");
                                        harPackingOptions.add(taskRoot);
                                        harPackingOptions.add(videoRoot);
                                        int ret = arch.run(Arrays.copyOf(harPackingOptions.toArray(),
                                                harPackingOptions.size(), String[].class));
                                        if (ret < 0) {
                                            throw new IOException("Packing tracklets for task "
                                                    + taskID + "(" + videoID + ") failed.");
                                        }
                                    }).execute();

                                    logger.info("Task " + taskID + "(" + videoID + ") packed!");

                                    // Set the HAR path to all the tracklets from this video.
                                    for (int i = 0; i < numTracklets; ++i) {
                                        new RobustExecutor<Integer, Void>((VoidFunction<Integer>) idx ->
                                                dbConnector.setTrackletSavingPath(
                                                        new Tracklet.Identifier(videoID, idx).toString(),
                                                        videoRoot + "/" + taskID + ".har/" + idx)).execute(i);
                                    }

                                    // Delete the original folder recursively.
                                    new RobustExecutor<Void, Void>(() ->
                                            new HDFSFactory().produce().delete(new Path(taskRoot), true)
                                    ).execute();
                                } else {
                                    logger.info("Task " + taskID + "(" + videoID + ") need "
                                            + (numTracklets - dirCnt + 1) + "/" + numTracklets + " more tracklets!");
                                }
                            } catch (Exception e) {
                                logger.error("On trying to pack tracklets", e);
                            }
                        });
                        final long end = System.currentTimeMillis();

                        try {
                            jobListener.commitSync();
                            if (records.count() >= maxPollRecords && end - start < MAX_POLL_INTERVAL_MS) {
                                // Can poll more records once, and there are many records in Kafka waiting to be processed.
                                maxPollRecords = maxPollRecords * 3 / 2;
                                consumerProperties.setProperty("max.poll.records", "" + maxPollRecords);
                                jobListener = new KafkaConsumer<>(consumerProperties);
                                jobListener.subscribe(Collections.singletonList(JOB_TOPIC));
                            }
                        } catch (CommitFailedException e) {
                            // Processing time is longer than poll interval.
                            // Poll fewer records once.
                            maxPollRecords /= 2;
                            consumerProperties.setProperty("max.poll.records", "" + maxPollRecords);
                            jobListener = new KafkaConsumer<>(consumerProperties);
                            jobListener.subscribe(Collections.singletonList(JOB_TOPIC));
                        }
                    }
                    hdfs.close();
                } catch (Exception e) {
                    logger.error("In packing thread", e);
                }
            }
        }
    }

    public static class TrackletSavingStream extends Stream {
        public static final String NAME = "tracklet-saving";
        public static final DataType OUTPUT_TYPE = DataType.NONE;
        public static final Port PED_TRACKLET_SAVING_PORT =
                new Port("pedestrian-tracklet-saving", DataType.TRACKLET);
        private static final long serialVersionUID = 2820895755662980265L;
        private final String metadataDir;
        
        // 测试ftp，暂时不保存数据库
//        private final Singleton<GraphDatabaseConnector> dbConnSingleton;  // Add by da.li
        
        // private final Singleton<ByteArrayProducer> packingJobProducerSingleton;  // Modified by da.li
        
        
        private Singleton<FtpUtil> ftpUtilSingleton;

        TrackletSavingStream(@Nonnull AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            metadataDir = propCenter.metadataDir;
            // Modified by da.li
            //packingJobProducerSingleton = new Singleton<>(
            //        new ByteArrayProducerFactory(propCenter.getKafkaProducerProp(false)),
            //        ByteArrayProducer.class);
            //dbConnector = new Neo4jConnector();
            
//            dbConnSingleton = new Singleton<>(Neo4jConnector::new, Neo4jConnector.class);
            
            ftpUtilSingleton=new Singleton<>(() ->FtpUtil.getInstance(), FtpUtil.class);
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
            // Save tracklets.
            this.filter(globalStreamMap, PED_TRACKLET_SAVING_PORT)
                    .foreachRDD(rdd -> rdd.foreachPartition(kvIter -> {
                        synchronized (TrackletSavingStream.class) {
                            final Logger logger = loggerSingleton.getInst();
                            Neo4jDaoJdbc neo4jDaoJdbc=neo4jDaoJdbcSingleton.getInst();
                            List<Data4Neo4j> list=new ArrayList<>();
//                          List<Data4Neo4j> list=new CopyOnWriteArrayList<>();
                            ParallelExecutor2.execute(kvIter, kv -> {
                            	FileSystem hdfs =HDFSFactory.newInstance();
                                try {
                                    final UUID taskID = kv._1();
                                    final TaskData taskData = kv._2();
                                    final TrackletOrURL trackletOrURL = (TrackletOrURL) taskData.predecessorRes;
                                    final Tracklet tracklet = trackletOrURL.getTracklet();
                                    String method="ssd";
                                    
                                    FtpUtil ftpUtil=ftpUtilSingleton.getInst();
                                    
                                    Ftp ftpProperties = ftpUtil.getFtp();
                            		FTPClient ftpClient = ftpUtil.getFTPClient(ftpProperties);
                            		//+ "/" + taskID+ "/" + tracklet.id.serialNumber
                            		String ftpDir = ftpProperties.getSysFile() +method+ dateString
                                     		+"/"+ tracklet.id.videoID.split("-")[0];
                            		//tracklet.id.toString()
                            		String nameString=tracklet.id.videoID+"_"+taskID+"_"+tracklet.id.serialNumber;
                             		String ftpFileName = new String((nameString+".jpg").getBytes("UTF-8"), "iso-8859-1");
                            		
                                    final int numTracklets = tracklet.numTracklets;
                                    final String userPlan = (String) taskData.userPlan;

                                    if (trackletOrURL.isStored()) {
                                        // The tracklet has already been stored at HDFS.
                                        logger.debug("Tracklet has already been stored at " + trackletOrURL.getURL());
                                        // TODO: Test following code on the system ...
                                        // Insert the saving path to database.
                                        new RobustExecutor<Void, Void>(() -> {
                                            String trackletInfo = 
                                                HadoopHelper.getTrackletInfo(trackletOrURL.getURL(), hdfs);
                                            long start=System.currentTimeMillis();
                                            if (ftpClient != null && ftpClient.isConnected()) {
                                    			boolean dirExist = ftpUtil.createDir(ftpDir, ftpClient);
                                    			logger.debug("if目录创建：" + dirExist);
                                    			if (dirExist) {

                                    				boolean fileExist = ftpUtil.isFTPFileExist(ftpDir, ftpFileName, ftpClient);
                                    				logger.debug("if文件是否存在：" + fileExist);
                                    				if (!fileExist) {

                                    					ftpUtil.generateImage(tracklet, ftpDir, ftpFileName, ftpClient);
                                    				}
                                    			}

                                    		}
                                            long end =System.currentTimeMillis();
                                            logger.debug("上传用时："+(end-start));
                                            // Insert to database.
                                            
//                                            dbConnSingleton.getInst().setPedestrianTracklet(
//                                                tracklet.id.toString(),
//                                                userPlan,
//                                                trackletOrURL.getURL(),
//                                                trackletInfo
//                                            );
                                            Data4Neo4j data4Neo4j=new Data4Neo4j();
                                            data4Neo4j.setNodeID(tracklet.id.toString());
                                            data4Neo4j.setDataType(userPlan);
                                            data4Neo4j.setTrackletPath(trackletOrURL.getURL());
                                            data4Neo4j.setTrackletInfo(trackletInfo);
                                            list.add(data4Neo4j);
//                                            neo4jDaoJdbc.setPedestrianTracklet(tracklet.id.toString(),
//                                                userPlan,
//                                                trackletOrURL.getURL(),
//                                                trackletInfo);
                                            
//                                            long saveEnd=System.currentTimeMillis();
//                                            logger.debug("保存tracking耗时："+(saveEnd-end));
                                        }).execute();
                                        logger.debug("Saving DONE: " + trackletOrURL.getURL());
                                    } else {
                                    	
                                        final String videoRoot = metadataDir + "result/"+method+"/" +dateString+"/"+ tracklet.id.videoID;
                                        final String taskRoot = videoRoot + "/" + taskID;
                                        final String storeDir = taskRoot + "/" + tracklet.id.serialNumber;
                                        final Path storePath = new Path(storeDir);
                                        logger.debug("保存的 Dir是: " + storeDir);
                                        new RobustExecutor<Void, Void>(() -> {
                                            if (hdfs.exists(storePath)|| hdfs.exists(new Path(videoRoot + "/" + taskID + ".har"))) {
                                                logger.warn("重复保存 request for " + tracklet.id);
                                            } else {
                                                hdfs.mkdirs(new Path(storeDir));
                                                //HadoopHelper.storeTracklet(storeDir, tracklet, hdfs);
                                                String trackletInfo = HadoopHelper.storeTrackletNew(
                                                    storeDir,
                                                    tracklet,
                                                    hdfs
                                                );
                                                long start=System.currentTimeMillis();
                                        		if (ftpClient != null && ftpClient.isConnected()) {
                                        			boolean dirExist = ftpUtil.createDir(ftpDir, ftpClient);
                                        			logger.debug("else目录创建：" + dirExist);
                                        			if (dirExist) {

                                        				boolean fileExist = ftpUtil.isFTPFileExist(ftpDir, ftpFileName, ftpClient);
                                        				logger.debug("else文件是否存在：" + fileExist);
                                        				if (!fileExist) {

                                        					ftpUtil.generateImage(tracklet, ftpDir, ftpFileName, ftpClient);
                                        				}
                                        			}else {
                                        				boolean dirExist2 = ftpUtil.createDir(ftpDir, ftpClient);
                                        				logger.debug("else再次目录创建2：" + dirExist2);
                                            			if (dirExist2) {

                                            				boolean fileExist2 = ftpUtil.isFTPFileExist(ftpDir, ftpFileName, ftpClient);
                                            				logger.debug("else再次文件是否存在2：" + fileExist2);
                                            				if (!fileExist2) {

                                            					ftpUtil.generateImage(tracklet, ftpDir, ftpFileName, ftpClient);
                                            				}
                                            			}
													}

                                        		}
                                        		long end =System.currentTimeMillis();
                                        		logger.debug("上传用时："+(end-start));
//                                                dbConnSingleton.getInst().setPedestrianTracklet(
//                                                    tracklet.id.toString(),
//                                                    userPlan,
//                                                    storeDir,
//                                                    trackletInfo
//                                                );
//                                                neo4jDaoJdbc.setPedestrianTracklet(
//                                                        tracklet.id.toString(),
//                                                        userPlan,
//                                                        storeDir,
//                                                        trackletInfo
//                                                    );
                                                Data4Neo4j data4Neo4j=new Data4Neo4j();
                                                data4Neo4j.setNodeID(tracklet.id.toString());
                                                data4Neo4j.setDataType(userPlan);
                                                data4Neo4j.setTrackletPath(storeDir);
                                                data4Neo4j.setTrackletInfo(trackletInfo);
                                                list.add(data4Neo4j);
//                                                long saveEnd=System.currentTimeMillis();
//                                                logger.debug("保存tracking耗时："+(saveEnd-end));
                                            }
                                        }).execute();
                                    }

                                    // Check packing. Commented by da.li 20170703
                                    /*
                                    new RobustExecutor<Void, Void>(() ->
                                            KafkaHelper.sendWithLog(TrackletPackingThread.JOB_TOPIC,
                                                    taskID.toString(),
                                                    serialize(new Tuple2<>(tracklet.id, numTracklets)),
                                                    packingJobProducerSingleton.getInst(),
                                                    logger)
                                    ).execute();
                                    */
//                                    hdfs.close();
                                } catch (Exception e) {
                                    logger.error("During storing tracklets.", e);
                                }finally {
									try {
										if (hdfs != null) {
											hdfs.close();
										}
									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
                            });
                            
                            if (list.size()>0) {
        						
                            	long start=System.currentTimeMillis();
                            	String lableName="SSD20181122";
                            	new RobustExecutor<Void, Void>(() ->
                            		neo4jDaoJdbc.setPedestrianTracklet(list,lableName)
                            	).execute();
                            	long end=System.currentTimeMillis();
                            	logger.debug("保存tracking："+list.size()+"个耗时："+(end-start));
                            	
                            	
                        		boolean b= neo4jDaoJdbc.setTrackletRelation(list,lableName);
                        		if (!b) {
                        			boolean b2=neo4jDaoJdbc.setTrackletRelation(list,lableName);
                        			if (b2) {
                        				logger.debug("再次保存关系成功");
									}else {
										logger.debug("再次保存关系失败");
										boolean b3=neo4jDaoJdbc.setTrackletRelation(list,lableName);
										if(b3){
											logger.debug("第三次保存关系成功");
										}else {
											logger.debug("第三次保存关系失败");
										}
										
									}
								}
	                        	long end2=System.currentTimeMillis();
	                        	logger.debug("保存关系："+list.size()+"个耗时："+(end2-end));
        					}
                        }
                    }));
        }

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(PED_TRACKLET_SAVING_PORT);
        }
    }

    public static class AttrSavingStream extends Stream {
        public static final String NAME = "attr-saving";
        public static final DataType OUTPUT_TYPE = DataType.NONE;
        public static final Port PED_ATTR_SAVING_PORT =
                new Port("pedestrian-attr-saving", DataType.ATTRIBUTES);
        private static final long serialVersionUID = 858443725387544606L;
//        private final Singleton<GraphDatabaseConnector> dbConnSingleton;

        AttrSavingStream(@Nonnull AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            //dbConnSingleton = new Singleton<>(FakeDatabaseConnector::new, FakeDatabaseConnector.class);
//            dbConnSingleton = new Singleton<>(Neo4jConnector::new, Neo4jConnector.class);
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
            // Display the attributes.
            // TODO Modify the streaming steps from here to store the meta data.
            this.filter(globalStreamMap, PED_ATTR_SAVING_PORT)
                    .foreachRDD(rdd -> rdd.foreachPartition(kvIter -> {
                        synchronized (AttrSavingStream.class) {
                            final Logger logger = loggerSingleton.getInst();
                            Neo4jDaoJdbc neo4jDaoJdbc=neo4jDaoJdbcSingleton.getInst();
                            List<Data4Neo4j> list=new ArrayList<>();
//                            List<Data4Neo4j> list=new CopyOnWriteArrayList<>();
                            ParallelExecutor2.execute(kvIter, res -> {
                                try {
                                    final TaskData taskData = res._2();
                                    final Attributes attr = (Attributes) taskData.predecessorRes;
                                    final String userPlan = (String) taskData.userPlan;
                                    Data4Neo4j data4Neo4j=new Data4Neo4j();
                                    data4Neo4j.setAttr(attr);
                                    data4Neo4j.setDataType(userPlan);
                                    data4Neo4j.setNodeID(attr.trackletID.toString());
                                    list.add(data4Neo4j);
//                                    long start=System.currentTimeMillis();
                                    logger.debug("Received 属性trackletID是：" + attr.trackletID.toString() );

//                                    new RobustExecutor<Void, Void>(() ->
////                                    neo4jDaoJdbcSingleton.getInst().setPedestrianAttributes(
////                                            attr.trackletID.toString(), userPlan, attr)
//                                            dbConnSingleton.getInst().setPedestrianAttributes(
//                                                attr.trackletID.toString(), userPlan, attr)
//                                    ).execute();

//                                    long end=System.currentTimeMillis();
//                                    logger.debug("Saved 属性" + ": trackletID是：" + attr.trackletID.toString() +",耗时："+(end-start));
                                } catch (Exception e) {
                                    logger.error("When decompressing attributes", e);
                                }
                            });
                            
                            if (list.size()>0) {
                            	String lableName="SSD20181122";
                            	long start=System.currentTimeMillis();
                            	new RobustExecutor<Void, Void>(() ->
                            		neo4jDaoJdbc.setPedestrianAttributes(list,lableName)
                            	).execute();
                            	long end=System.currentTimeMillis();
                            	logger.debug("Saved 属性："+list.size()+"个耗时："+(end-start));
        					}
                        }
                    }));
        }

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(PED_ATTR_SAVING_PORT);
        }
    }

    /**
     * Class: ReidFeatureSavingStream
     * Add by da.li on 2017/06/23 for saving extracted features.
     */
    public static class ReidFeatureSavingStream extends Stream {
    	private static final long serialVersionUID = 3468177053696762940L;
        public static final String NAME = "reid-feature-saving";
        public static final DataType OUTPUT_TYPE = DataType.NONE;
        public static final Port PED_REID_FEATURE_SAVING_PORT =
            new Port("pedestrian-reid-feature-saving", DataType.REID_FEATURE);
//        private final Singleton<GraphDatabaseConnector> dbConnSingleton;
//        public Singleton<Neo4jDaoJdbc> neo4jDaoJdbcSingleton;
        public ReidFeatureSavingStream(@Nonnull AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

//            dbConnSingleton = new Singleton<>(Neo4jConnector::new, Neo4jConnector.class);
            
        }

        @Override
        public void addToGlobalStream(Map<DataType, JavaPairDStream<UUID, TaskData>> globalStreamMap) {
            this.filter(globalStreamMap, PED_REID_FEATURE_SAVING_PORT)
                .foreachRDD(rdd -> rdd.foreachPartition(kvIter -> {
                synchronized (ReidFeatureSavingStream.class) {
                    final Logger logger = loggerSingleton.getInst();
                    Neo4jDaoJdbc neo4jDaoJdbc=neo4jDaoJdbcSingleton.getInst();
                    List<Data4Neo4j> list=new ArrayList<>();
//                    List<Data4Neo4j> list=new CopyOnWriteArrayList<>();
                    ParallelExecutor2.execute(kvIter, res -> {
                        try {
                            final TaskData taskData = res._2();
                            final Feature fea = (Feature) taskData.predecessorRes;
                            final String userPlan = (String) taskData.userPlan;
                            logger.debug("Received REID"  + ": trackletID是：" + fea.trackletID.toString());
                            Data4Neo4j data4Neo4j=new Data4Neo4j();
                            data4Neo4j.setDataType(userPlan);
                            data4Neo4j.setFea(fea);
                            data4Neo4j.setNodeID(fea.trackletID.toString());
                            list.add(data4Neo4j);
                            // TODO: insert the recieved reid feature to db.
                        } catch(Exception e) {
                            logger.error("When decompressing reid feature", e);
                        }
                    });
                    if (list.size()>0) {
                    	String lableName="SSD20181122";
                    	long start=System.currentTimeMillis();
                    	new RobustExecutor<Void, Void>(() ->
//                                dbConnSingleton.getInst().setPedestrianReIDFeature(
//                                    fea.trackletID.toString(), userPlan, fea)
                    		neo4jDaoJdbc.setPedestrianReIDFeature(list,lableName)
                    	).execute();
                    	long end=System.currentTimeMillis();
//                    logger.debug("Saved REID"  + ": trackletID是：" + fea.trackletID.toString()+",耗时："+(end-start));
                    	logger.debug("Saved REID："+list.size()+"个耗时："+(end-start));
					}
                }
            }));
        }

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(PED_REID_FEATURE_SAVING_PORT);
        }
    }

    
    public static class PedestrianDetectionSavingStream extends Stream {
        /**
		 * 
		 */
		private static final long serialVersionUID = 2804103177410930691L;
		public static final String NAME = "detection-saving";
        public static final DataType OUTPUT_TYPE = DataType.NONE;
        public static final Port PED_DETECTION_SAVING_PORT =
            new Port("pedestrian-detection-saving", DataType.DETECTION);
        private final String metadataDir;
        
//        private final Singleton<GraphDatabaseConnector> dbConnSingleton;
//        public Singleton<Neo4jDaoJdbc> neo4jDaoJdbcSingleton;
        public PedestrianDetectionSavingStream(@Nonnull AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);
            metadataDir = propCenter.metadataDir;
//            dbConnSingleton = new Singleton<>(Neo4jConnector::new, Neo4jConnector.class);
            
        }

        @Override
        public void addToGlobalStream(Map<DataType, JavaPairDStream<UUID, TaskData>> globalStreamMap) {
            this.filter(globalStreamMap, PED_DETECTION_SAVING_PORT)
                .foreachRDD(rdd -> rdd.foreachPartition(kvIter -> {
                synchronized (PedestrianDetectionSavingStream.class) {
                	final Logger logger = loggerSingleton.getInst();
                    Neo4jDaoJdbc neo4jDaoJdbc=neo4jDaoJdbcSingleton.getInst();
                    List<Data4Neo4j> list=new ArrayList<>();
//                  List<Data4Neo4j> list=new CopyOnWriteArrayList<>();
                    ParallelExecutor2.execute(kvIter, kv -> {
                    	FileSystem hdfs =HDFSFactory.newInstance();
                        try {
                            final UUID taskID = kv._1();
                            final TaskData taskData = kv._2();
                            final TrackletOrURL trackletOrURL = (TrackletOrURL) taskData.predecessorRes;
                            final Tracklet tracklet = trackletOrURL.getTracklet();
                            String method="motion";
                            
//                            FtpUtil ftpUtil=ftpUtilSingleton.getInst();
                            
//                            Ftp ftpProperties = ftpUtil.getFtp();
//                    		FTPClient ftpClient = ftpUtil.getFTPClient(ftpProperties);
//                    		String ftpDir = ftpProperties.getSysFile() +method+ "-"+dateString
//                             		+"/"+ tracklet.id.videoID+ "/" + taskID+ "/" + tracklet.id.serialNumber;
//                     		String ftpFileName = new String((tracklet.id.toString()+".jpg").getBytes("UTF-8"), "iso-8859-1");
                    		
                            final int numTracklets = tracklet.numTracklets;
                            final String userPlan = (String) taskData.userPlan;

                            if (trackletOrURL.isStored()) {
                                // The tracklet has already been stored at HDFS.
                                logger.debug("Tracklet has already been stored at " + trackletOrURL.getURL());
                                // TODO: Test following code on the system ...
                                // Insert the saving path to database.
                                new RobustExecutor<Void, Void>(() -> {
                                    String trackletInfo = 
                                        HadoopHelper.getTrackletInfo(trackletOrURL.getURL(), hdfs);
                                    long start=System.currentTimeMillis();
//                                    if (ftpClient != null && ftpClient.isConnected()) {
//                            			boolean dirExist = ftpUtil.createDir(ftpDir, ftpClient);
//                            			System.out.println("if目录创建：" + dirExist);
//                            			if (dirExist) {
//
//                            				boolean fileExist = ftpUtil.isFTPFileExist(ftpDir, ftpFileName, ftpClient);
//                            				System.out.println("if文件是否存在：" + fileExist);
//                            				if (!fileExist) {
//
//                            					ftpUtil.generateImage(tracklet, ftpDir, ftpFileName, ftpClient);
//                            				}
//                            			}
//
//                            		}
                                    long end =System.currentTimeMillis();
                                    System.out.println("上传用时："+(end-start));
                                    // Insert to database.
                                    
//                                    dbConnSingleton.getInst().setPedestrianTracklet(
//                                        tracklet.id.toString(),
//                                        userPlan,
//                                        trackletOrURL.getURL(),
//                                        trackletInfo
//                                    );
                                    Data4Neo4j data4Neo4j=new Data4Neo4j();
                                    data4Neo4j.setNodeID(tracklet.id.toString());
                                    data4Neo4j.setDataType(userPlan);
                                    data4Neo4j.setTrackletPath(trackletOrURL.getURL());
                                    data4Neo4j.setTrackletInfo(trackletInfo);
                                    list.add(data4Neo4j);
//                                    neo4jDaoJdbc.setPedestrianTracklet(tracklet.id.toString(),
//                                        userPlan,
//                                        trackletOrURL.getURL(),
//                                        trackletInfo);
                                    
//                                    long saveEnd=System.currentTimeMillis();
//                                    logger.debug("保存tracking耗时："+(saveEnd-end));
                                }).execute();
                                logger.debug("Saving DONE: " + trackletOrURL.getURL());
                            } else {
                            	
                                final String videoRoot = metadataDir + "result/"+method+"/" +dateString+"/"+ tracklet.id.videoID;
                                final String taskRoot = videoRoot + "/" + taskID;
                                final String storeDir = taskRoot + "/" + tracklet.id.serialNumber;
                                final Path storePath = new Path(storeDir);
                                logger.debug("保存的 Dir是: " + storeDir);
                                new RobustExecutor<Void, Void>(() -> {
                                    if (hdfs.exists(storePath)|| hdfs.exists(new Path(videoRoot + "/" + taskID + ".har"))) {
                                        logger.warn("重复保存 request for " + tracklet.id);
                                    } else {
                                        hdfs.mkdirs(new Path(storeDir));
                                        //HadoopHelper.storeTracklet(storeDir, tracklet, hdfs);
                                        String trackletInfo = HadoopHelper.storeTrackletNew(
                                            storeDir,
                                            tracklet,
                                            hdfs
                                        );
                                        long start=System.currentTimeMillis();
//                                		if (ftpClient != null && ftpClient.isConnected()) {
//                                			boolean dirExist = ftpUtil.createDir(ftpDir, ftpClient);
//                                			System.out.println("else目录创建：" + dirExist);
//                                			if (dirExist) {
//
//                                				boolean fileExist = ftpUtil.isFTPFileExist(ftpDir, ftpFileName, ftpClient);
//                                				System.out.println("else文件是否存在：" + fileExist);
//                                				if (!fileExist) {
//
//                                					ftpUtil.generateImage(tracklet, ftpDir, ftpFileName, ftpClient);
//                                				}
//                                			}else {
//                                				boolean dirExist2 = ftpUtil.createDir(ftpDir, ftpClient);
//                                    			System.out.println("else再次目录创建2：" + dirExist2);
//                                    			if (dirExist2) {
//
//                                    				boolean fileExist2 = ftpUtil.isFTPFileExist(ftpDir, ftpFileName, ftpClient);
//                                    				System.out.println("else再次文件是否存在2：" + fileExist2);
//                                    				if (!fileExist2) {
//
//                                    					ftpUtil.generateImage(tracklet, ftpDir, ftpFileName, ftpClient);
//                                    				}
//                                    			}
//											}
//
//                                		}
                                		long end =System.currentTimeMillis();
                                        System.out.println("上传用时："+(end-start));
//                                        dbConnSingleton.getInst().setPedestrianTracklet(
//                                            tracklet.id.toString(),
//                                            userPlan,
//                                            storeDir,
//                                            trackletInfo
//                                        );
//                                        neo4jDaoJdbc.setPedestrianTracklet(
//                                                tracklet.id.toString(),
//                                                userPlan,
//                                                storeDir,
//                                                trackletInfo
//                                            );
                                        Data4Neo4j data4Neo4j=new Data4Neo4j();
                                        data4Neo4j.setNodeID(tracklet.id.toString());
                                        data4Neo4j.setDataType(userPlan);
                                        data4Neo4j.setTrackletPath(storeDir);
                                        data4Neo4j.setTrackletInfo(trackletInfo);
                                        list.add(data4Neo4j);
//                                        long saveEnd=System.currentTimeMillis();
//                                        logger.debug("保存tracking耗时："+(saveEnd-end));
                                    }
                                }).execute();
                            }

                            // Check packing. Commented by da.li 20170703
                            /*
                            new RobustExecutor<Void, Void>(() ->
                                    KafkaHelper.sendWithLog(TrackletPackingThread.JOB_TOPIC,
                                            taskID.toString(),
                                            serialize(new Tuple2<>(tracklet.id, numTracklets)),
                                            packingJobProducerSingleton.getInst(),
                                            logger)
                            ).execute();
                            */
//                            hdfs.close();
                        } catch (Exception e) {
                            logger.error("During storing tracklets.", e);
                        }finally {
							try {
								if (hdfs != null) {
									hdfs.close();
								}
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
                    });
                    
                    if (list.size()>0) {
						
                    	long start=System.currentTimeMillis();
                    	String lableName="motion20180616";
                    	new RobustExecutor<Void, Void>(() ->
                    		neo4jDaoJdbc.setPedestrianTracklet(list,lableName)
                    	).execute();
                    	long end=System.currentTimeMillis();
                    	logger.debug("保存Detection："+list.size()+"个耗时："+(end-start));
                    	
                    	
                		boolean b= neo4jDaoJdbc.setTrackletRelation(list,lableName);
                		if (!b) {
                			boolean b2=neo4jDaoJdbc.setTrackletRelation(list,lableName);
                			if (b2) {
								System.out.println("再次保存关系成功");
							}else {
								System.out.println("再次保存关系失败");
								boolean b3=neo4jDaoJdbc.setTrackletRelation(list,lableName);
								if(b3){
									System.out.println("第三次保存关系成功");
								}else {
									System.out.println("第三次保存关系失败");
								}
								
							}
						}
                    	long end2=System.currentTimeMillis();
                    	logger.debug("保存Detection关系："+list.size()+"个耗时："+(end2-end));
					}
                }
            }));
}

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(PED_DETECTION_SAVING_PORT);
        }
    }
    
    public static class IDRankSavingStream extends Stream {
        public static final String NAME = "idrank-saving";
        public static final DataType OUTPUT_TYPE = DataType.NONE;
        public static final Port PED_IDRANK_SAVING_PORT =
                new Port("pedestrian-idrank-saving", DataType.IDRANK);
        private static final long serialVersionUID = -6469177153696762040L;

        public IDRankSavingStream(@Nonnull AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);
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
            // Display the id ranks.
            // TODO Modify the streaming steps from here to store the meta data.
            this.filter(globalStreamMap, PED_IDRANK_SAVING_PORT)
                    .foreachRDD(rdd -> rdd.foreachPartition(kvIter -> {
                        synchronized (IDRankSavingStream.class) {
                            final Logger logger = loggerSingleton.getInst();
                            ParallelExecutor.execute(kvIter, kv -> {
                                try {
                                    final TaskData taskData = kv._2();
                                    final int[] idRank = (int[]) taskData.predecessorRes;
                                    String rankStr = "";
                                    for (int id : idRank) {
                                        rankStr = rankStr + id + " ";
                                    }
                                    logger.info("Metadata saver received: " + kv._1()
                                            + ": Pedestrian IDRANK rank: " + rankStr);
                                    //TODO(Ken Yu): Save IDs to database.
                                } catch (Exception e) {
                                    logger.error("When decompressing IDRANK", e);
                                }
                            });
                        }
                    }));
        }

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(PED_IDRANK_SAVING_PORT);
        }
    }

    @Override
    public void addToContext() throws Exception {
        // Do nothing.
    }
}
