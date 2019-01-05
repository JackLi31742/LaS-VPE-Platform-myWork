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

package org.cripac.isee.vpe.common;

import static org.cripac.isee.util.SerializationHelper.deserialize;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.apache.spark.util.LongAccumulator;
import org.cripac.isee.util.Singleton;
import org.cripac.isee.util.WebToolUtils;
import org.cripac.isee.vpe.ctrl.MonitorThread;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskController;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.entities.Report;
import org.cripac.isee.vpe.entities.ServerInfo;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import scala.Tuple2;

/**
 * The SparkStreamingApp class wraps a whole Spark Streaming application,
 * including driver code and executor code. After initialized, it can be used
 * just like a JavaStreamingContext class. Note that you must call the
 * initialize() method after construction and before using it.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public abstract class SparkStreamingApp implements Serializable {

    private static final long serialVersionUID = 3098753124157119358L;
    @Nonnull
    private SystemPropertyCenter propCenter;
    @Nonnull
    private final String appName;

    public static int pid;
//    public static Report report ;
    private String masterName="cpu-master-nod";
    public static final String REPORT_TOPIC = "monitor-desc-";
    /**
     * Kafka parameters for creating input streams pulling messages from Kafka
     * Brokers.
     */
    @Nonnull
    public final Map<String, Object> kafkaParams;

    public SparkStreamingApp(@Nonnull SystemPropertyCenter propCenter,
                             @Nonnull String appName) throws Exception {
        this.propCenter = propCenter;
        this.appName = appName;
        this.kafkaParams = propCenter.getKafkaParams(appName);
        this.loggerSingleton = new Singleton<>(
                new SynthesizedLoggerFactory(appName, propCenter),
                SynthesizedLogger.class);
        this.monitorSingleton = new Singleton<>(() -> {
            MonitorThread monitorThread = new MonitorThread(loggerSingleton.getInst(), propCenter);
            monitorThread.start();
            return monitorThread;
        }, MonitorThread.class);
        
//        GpuManage gpuManage=new GpuManage();
//        report =gpuManage.get();
        if (propCenter.taskControllerEnable) {
            this.taskController = new Singleton<>(() -> {
                TaskController taskController = new TaskController(propCenter, loggerSingleton.getInst());
                taskController.start();
                return taskController;
            }, TaskController.class);
        }
        //得到的是AM Container的pid
//        pid=WebToolUtils.getPID();
//        List<Integer> gpuList=new ArrayList<>();
//        gpuList.add(0);
//        propCenter.gpus=gpuList ;
//        propCenter.gpus=getGpus() ;
        String hostname=WebToolUtils.getLocalHostName();
        System.out.println("SparkStreamingApp中的hostname是："+hostname);
//        HadoopHelper.getFileSystem();
    }
    
    /*public Collection<Integer> getGpus() {
    	GpuManage gpuManage=new GpuManage();
    	Report report =gpuManage.initReport();
    	ServerInfo serverInfo=gpuManage.getServerInfoReport(report);
    	gpuManage.getDevInfo(report,serverInfo);
    	List<Integer> devNumList=serverInfo.devNumList;
        List<Integer> processNumList=serverInfo.processNumAllList;
        
        Collection<Integer> result=CollectionUtils.subtract(devNumList, processNumList);  
        result.forEach(f->{System.out.println("服务器上的gpu是："+f);});
        return result;
    }*/
    
    /**
     * 空指針，暫時不使用
     * LANG
     * @return
     * @throws YarnException
     * @throws IOException
     */
    public Collection<Integer> getGpusInfo() throws YarnException, IOException{
    	Type serverInfosMapType = new TypeToken<Map <String,ServerInfo>>() {}.getType();
    	KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(getKafkaConsumerProp(true));
    	Configuration conf = getConfiguration();
    	Report reportAll=new Report();
		reportAll.serverInfosMap=new HashMap<String, ServerInfo>();
		YarnClient yarnClient=getYarnClient(conf);
		List<String> nodeNamesList=getNodesName(yarnClient);
		List<String> topicList=new ArrayList<String>();
		topicList.addAll(nodeNamesList);
		kafkaConsumer.subscribe(topicList);
		try {
			
//			monitorDemo.basic(kafkaConsumer,reportAll,hdfs);
//			while (true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
				for (ConsumerRecord<String, String> consumerRecord : records) {
					String key = consumerRecord.key();
					String value = consumerRecord.value();
					System.out.println("key:" + key );
					if (nodeNamesList.contains(key)) {
						if (reportAll.serverInfosMap!=null) {
							if (reportAll.serverInfosMap.size()>1) {
								
								for (Entry<String,ServerInfo> entry : reportAll.serverInfosMap.entrySet()) {
									String entryKey=entry.getKey();
									ServerInfo entryValue=(ServerInfo)entry.getValue();
									if (entryKey.equals(key)) {
										reportAll.serverInfosMap.remove(entryKey, entryValue);
									}
								}
							}
						}
						reportAll.serverInfosMap
							.putAll((Map <String,ServerInfo>)new Gson().fromJson(value, serverInfosMapType));
					}
				}
//			}
			
		}catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			// TODO: handle finally clause
//			kafkaConsumer.close();
		}
		Map<String, ServerInfo> serverInfosMap=reportAll.serverInfosMap;
		String nodeName = getServerName();
		ServerInfo serverInfo=serverInfosMap.get(nodeName);
		
		List<Integer> devNumList=serverInfo.devNumList;
        List<Integer> processNumList=serverInfo.processNumAllList;
        
        Collection<Integer> result=CollectionUtils.subtract(devNumList, processNumList);  
        result.forEach(f->{System.out.println("服务器上的gpu是："+f);});
        return result;
    }
    public String getServerName(){
    	String nodeName1;
        try {
            nodeName1 = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            nodeName1 = "Unknown host";
            e.printStackTrace();
        }
        return nodeName1;
    }

    public Properties getKafkaConsumerProp( boolean isStringValue) {
        Properties consumerProp = new Properties();
        consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "gpu-task-nod2:9092,gpu-task-nod1:9092,gpu-task-nod5:9092");
        consumerProp.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaCus");
        consumerProp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerProp.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumerProp.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        consumerProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                isStringValue ? StringDeserializer.class : ByteArrayDeserializer.class);
        consumerProp.put("request.timeout.ms", 60000);
        return consumerProp;
    }
    
	public Configuration getConfiguration(){
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://"+masterName+":8020");
		conf.set("yarn.resourcemanager.scheduler.address", masterName+":8030");
		conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		String hadoopHome = System.getenv("HADOOP_HOME");
		conf.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
		conf.addResource(new Path(hadoopHome + "/etc/hadoop/yarn-site.xml"));
		conf.setBoolean("dfs.support.append", true);
//      conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName(), "LaS-VPE-Platform-Web");
//      conf.set("fs.file.impl", LocalFileSystem.class.getName(), "LaS-VPE-Platform-Web");
		return conf;
	}
	public YarnClient getYarnClient(Configuration conf){
//		YarnConfiguration conf = new YarnConfiguration();
////		conf.set("fs.defaultFS", "hdfs://rtask-nod8:8020");
//		conf.set("yarn.resourcemanager.scheduler.address", "rtask-nod8:8030");
//		String hadoopHome = System.getenv("HADOOP_HOME");
//		conf.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
//		conf.addResource(new Path(hadoopHome + "/etc/hadoop/yarn-site.xml"));
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();
		return yarnClient;
	}
	public List<String> getNodesName(YarnClient yarnClient) throws YarnException, IOException{
		List<NodeReport> nodeList=yarnClient.getNodeReports(NodeState.RUNNING);
		List<String> nodeNamesList=new ArrayList<String>();
		for (int i = 0; i < nodeList.size(); i++) {
			
			NodeReport nodeReport=nodeList.get(i);
			nodeNamesList.add(REPORT_TOPIC+nodeReport.getNodeId().getHost());
			
		}
		return nodeNamesList;
	}
    /**
     * Common Spark Streaming context variable.
     */
    private transient JavaStreamingContext jssc = null;
    
//    private static volatile LongAccumulator accumulator = null;

    @Nonnull
    protected final Singleton<Logger> loggerSingleton;

    @Nonnull
    private final List<Stream> streams = new ArrayList<>();

    @Nonnull
    private final Singleton<MonitorThread> monitorSingleton;
    

    @Nullable
    private Singleton<TaskController> taskController = null;

    protected void registerStreams(Collection<Stream> streams) {
        this.streams.addAll(streams);
    }

    /**
     * Utility function for all applications to receive messages with byte
     * array values from Kafka with direct stream.
     *
     * @param acceptingTypes Data types the stream accepts.
     * @param repartition    Number of partitions when repartitioning the RDDs.
     *                       -1 means do not do repartition. 0 means using default parallelism of Spark.
     * @return A Kafka non-receiver input stream.
     */
    @Nonnull
    protected JavaPairDStream<DataType, Tuple2<String, byte[]>> buildDirectStream(@Nonnull Collection<DataType> acceptingTypes,
                      int repartition) throws SparkException {
		final JavaInputDStream<ConsumerRecord<String, byte[]>> inputDStream = KafkaUtils.createDirectStream(
				jssc,
				propCenter.kafkaLocationStrategy.equals("PreferBrokers") ? LocationStrategies.PreferBrokers(): LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(acceptingTypes.stream().map(Enum::name).collect(Collectors.toList()),
						kafkaParams));

        JavaPairDStream<DataType, Tuple2<String, byte[]>> stream = inputDStream
                // Manipulate offsets.
                .transform(rdd -> {
                    final Logger logger = loggerSingleton.getInst();
                    if (!monitorSingleton.getInst().isAlive()) {
                        logger.error("Monitor is dead!");
                    }

                    // Store offsets.
                    final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

                    // Directly commit the offsets, since data has been checkpointed in Spark Streaming.
                    ((CanCommitOffsets) inputDStream.inputDStream()).commitAsync(offsetRanges);

                    // Find offsets which indicate new messages have been received.
                    rdd.foreachPartition(consumerRecords -> {
                        final Logger executorLogger = loggerSingleton.getInst();
                        if (!monitorSingleton.getInst().isAlive()) {
                            executorLogger.error("Monitor is dead!");
                        }
                        final OffsetRange o = offsetRanges[TaskContext.getPartitionId()];
                        if (o.fromOffset() < o.untilOffset()) {
                            executorLogger.debug("Received {topic=" + o.topic()
                                    + ", partition=" + o.partition()
                                    + ", fromOffset=" + o.fromOffset()
                                    + ", untilOffset=" + o.untilOffset() + "}");
                        }
                    });
                    int numNewMessages = 0;
                    for (OffsetRange o : offsetRanges) {
                        numNewMessages += o.untilOffset() - o.fromOffset();
                    }
                    if (numNewMessages == 0) {
//                        logger.debug("No new messages!");
                    } else {
                        logger.debug("Received " + numNewMessages + " messages totally.");
                    }
                    return rdd;
                })
                .mapToPair(rec -> new Tuple2<>(DataType.valueOf(rec.topic()),
                        new Tuple2<>(rec.key(), rec.value())));

        if (repartition >= 0) {
            // Repartition the records.
            stream = stream.repartition(repartition == 0 ? jssc.sparkContext().defaultParallelism() : repartition);
        }

        return stream;
    }

    /**
     * Utility function for all applications to receive messages with byte
     * array values from Kafka with direct stream. RDDs are repartitioned by default.
     *
     * @param acceptingTypes Data types the stream accepts.
     * @return A Kafka non-receiver input stream.
     */
    @Nonnull
    protected JavaPairDStream<DataType, Tuple2<String, byte[]>> buildDirectStream
    					(@Nonnull Collection<DataType> acceptingTypes) throws SparkException {
        return buildDirectStream(acceptingTypes, propCenter.repartition);
    }

    /**
     * Add streaming actions directly to the global streaming context.
     * This is for applications that may take as input messages with types other than {@link TaskData}.
     * Actions that take {@link TaskData} as input should be implemented in the
     * {@link Stream#addToGlobalStream(Map)}, in order to save time of deserialization.
     * Note that existence of Kafka topics used in this method is not automatically checked.
     */
    public abstract void addToContext() throws Exception;

    /**
     * Initialize the application.
     */
    public void initialize() {
        KafkaHelper.checkTopics(propCenter.zkConn,
                propCenter.zkSessionTimeoutMs,
                propCenter.zkConnectionTimeoutMS,
                propCenter.kafkaNumPartitions,
                propCenter.kafkaReplFactor);

        final Collection<DataType> acceptingTypes = streams.stream()
                .flatMap(stream -> stream.getPorts().stream().map(port -> port.inputType))
                .collect(Collectors.toList());

        String checkpointDir = propCenter.checkpointRootDir + "/" + appName;
        jssc = JavaStreamingContext.getOrCreate(checkpointDir, () -> {
            // Load default Spark configurations.
            SparkConf sparkConf = new SparkConf(true)
                    .set("spark.executor.memory", propCenter.executorMem)
                    .set("spark.executor.instances", "" + propCenter.numExecutors)
                    // Register custom classes with Kryo.
                    .registerKryoClasses(new Class[]{TaskData.class, DataType.class});
            if (propCenter.maxRatePerPartition != null) {
                // Set maximum number of messages per second that each partition will accept
                // in the direct Kafka input stream.
                sparkConf = sparkConf
                        .set("spark.streaming.kafka.maxRatePerPartition", "" + propCenter.maxRatePerPartition);
            }
            // Create contexts.
            JavaSparkContext jsc = new JavaSparkContext(sparkConf);
            jsc.setLocalProperty("spark.scheduler.pool", "vpe");
            jssc = new JavaStreamingContext(jsc, Durations.milliseconds(propCenter.batchDuration));

            addToContext();

            if (!acceptingTypes.isEmpty()) {
//            	long start=System.currentTimeMillis();
//            	System.out.println("SparkStreamingApp开始接收数据:"+start);
                JavaPairDStream<DataType, Tuple2<UUID, byte[]>> inputStream =
                        buildDirectStream(acceptingTypes)
                                .mapValues(tuple -> new Tuple2<>(UUID.fromString(tuple._1()), tuple._2()));
                if (taskController != null) {
                    inputStream = inputStream.filter(kv ->
                            (Boolean) !taskController.getInst().termSigPool.contains(kv._2()._1()));
                }
                
                Map<DataType, JavaPairDStream<UUID, TaskData>> streamMap = new HashMap<>();
                for (DataType type : acceptingTypes) {
                    streamMap.put(type,
                            inputStream.filter(rec -> (Boolean) (Objects.equals(rec._1(), type)))
                                    .mapToPair(rec -> new Tuple2<UUID,TaskData>(rec._2()._1(), deserialize(rec._2()._2()))));
                }
                
//                long end=System.currentTimeMillis();
//                System.out.println("SparkStreamingApp接收数据结束:"+end);
//                System.out.println("SparkStreamingApp接收kafka用时:"+(end-start));
                /*List<Integer> list=new ArrayList<>();
        		list.add(0);
        		if (getServerName().equals("gpu-task-nod2")) {
        			list.add(1);
        		}
        		Collection<Integer> gpus=list;
        		JavaRDD<Object> gpuRdd=jsc.parallelize(Arrays.asList(gpus.toArray()));
                System.out.println("SparkStreamingApp里的gpus是："+gpus);
                java.util.Queue<JavaRDD<Object>> queue = new LinkedList<JavaRDD<Object>>();
                queue.add( gpuRdd );
                JavaDStream<Object> gpuDstream =jssc.queueStream(queue);*/
                
//                accumulator = jsc.sc().longAccumulator();
                streams.forEach(stream -> stream.addToGlobalStream(streamMap));
//                System.out.println("SparkStreamingApp计时器里面的值:"+accumulator.value());
                
//                streams.forEach(stream -> stream.addToGlobalStream(streamMap,gpuDstream));
                /*if (streamMap.size()>0) {
					System.out.println("SparkStreamingApp处理结束-------------------");
				}*/
            }

            try {
                if (propCenter.sparkMaster.contains("local")) {
                    File dir = new File(checkpointDir);
                    //noinspection ResultOfMethodCallIgnored
                    dir.delete();
                    //noinspection ResultOfMethodCallIgnored
                    dir.mkdirs();
                } else {
                    FileSystem fs = FileSystem.get(new Configuration());
                    Path dir = new Path(checkpointDir);
                    fs.delete(dir, true);
                    fs.mkdirs(dir);
                }
                jssc.checkpoint(checkpointDir);
            } catch (IllegalArgumentException | IOException e) {
                e.printStackTrace();
            }
            return jssc;
        }, new Configuration(), true);
    }

    /**
     * Start the application.
     */
    public void start() {
        jssc.start();
    }

    /**
     * Stop the application.
     */
    public void stop() {
        jssc.stop();
    }

    /**
     * Await termination of the application.
     */
    public void awaitTermination() {
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (jssc != null) {
            jssc.close();
        }
        super.finalize();
    }
    
    
}
