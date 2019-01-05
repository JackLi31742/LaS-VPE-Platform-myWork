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

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.apache.kafka.common.KafkaException;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.cripac.isee.util.Singleton;
import org.cripac.isee.vpe.ctrl.GpuManage;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.data.MysqlDaoJdbc;
import org.cripac.isee.vpe.data.Neo4jDaoJdbc;
import org.cripac.isee.vpe.entities.Report;
import org.cripac.isee.vpe.util.kafka.ByteArrayProducer;
import org.cripac.isee.vpe.util.kafka.ByteArrayProducerFactory;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;

import kafka.common.FailedToSendMessageException;
import kafka.common.MessageSizeTooLargeException;

/**
 * A Stream is a flow of DStreams. Each stream outputs at most one type of data.
 * <p>
 * Created by ken.yu on 16-10-26.
 */
public abstract class Stream implements Serializable {
    private static final long serialVersionUID = 7965952554107861881L;
    private final Singleton<ByteArrayProducer> producerSingleton;
    private final boolean verbose;
    
    // Modified by da.li  -->  Add userPlan (a string).
    protected void output(Collection<TaskData.ExecutionPlan.Node.Port> outputPorts,
           TaskData.ExecutionPlan executionPlan,
           Serializable result,
           Serializable userPlan,
           UUID taskID) throws Exception {
        new RobustExecutor<Void, Void>(
                () -> {
                    if (verbose) {
                        KafkaHelper.sendWithLog(taskID.toString(),
                                new TaskData(outputPorts, executionPlan, result, userPlan),
                                producerSingleton.getInst(),
                                loggerSingleton.getInst());
                    } else {
                        KafkaHelper.send(taskID.toString(),
                                new TaskData(outputPorts, executionPlan, result, userPlan),
                                producerSingleton.getInst());
                    }
                },
                Arrays.asList(
                        MessageSizeTooLargeException.class,
                        KafkaException.class,
                        FailedToSendMessageException.class)
        ).execute();
    }

    protected JavaPairDStream<UUID, TaskData> filter(Map<DataType, JavaPairDStream<UUID, TaskData>> streamMap, Port port) {
        return streamMap.get(port.inputType)
                .filter(rec -> (Boolean) rec._2().destPorts.containsKey(port));
    }

    protected final Singleton<Logger> loggerSingleton;
    public Singleton<Report>  reportSingleton ;
    public Singleton<MysqlDaoJdbc> mysqlSingleton; 
    public Singleton<Neo4jDaoJdbc> neo4jDaoJdbcSingleton;
    public String dateString;
//    public Singleton<FileSystem> fsSingleton;
//    public Singleton<GpuManage>  gpuManageSingleton ;

    /**
     * Initialize necessary components of a Stream object.
     *
     * @param appName    Enclosing application name.
     * @param propCenter System property center.
     * @throws Exception On failure creating singleton.
     */
    public Stream(String appName, SystemPropertyCenter propCenter) throws Exception {
        this.verbose = propCenter.verbose;

        this.loggerSingleton = new Singleton<>(new SynthesizedLoggerFactory(appName, propCenter), SynthesizedLogger.class);

        Properties producerProp = propCenter.getKafkaProducerProp(false);
        producerSingleton = new Singleton<>(new ByteArrayProducerFactory(producerProp), ByteArrayProducer.class);
//        GpuManage gpuManage=new GpuManage();
//        report =gpuManage.get();
        this.reportSingleton=new Singleton<>(()->new GpuManage().get(),Report.class);
        this.mysqlSingleton=new Singleton<>(()->new MysqlDaoJdbc(), MysqlDaoJdbc.class);
        this.neo4jDaoJdbcSingleton=new Singleton<>(()->new Neo4jDaoJdbc(), Neo4jDaoJdbc.class);
//        this.dateString=getDate();
        this.dateString="20181122";
//        this.fsSingleton=new Singleton<>(()->{FileSystem fs=HadoopHelper.getFileSystem();return fs;},FileSystem.class);
    }

    /**
     * Add streaming actions to the global {@link TaskData} stream.
     * This global stream contains pre-deserialized TaskData messages, so as to save time.
     *
     * @param globalStreamMap A map of streams. The key of an entry is the topic name,
     *                        which must be one of the {@link DataType}.
     *                        The value is a filtered stream.
     */
    public abstract void addToGlobalStream(Map<DataType, JavaPairDStream<UUID, TaskData>> globalStreamMap);
    /*public abstract void addToGlobalStream(Map<DataType, JavaPairDStream<UUID, TaskData>> globalStreamMap,JavaDStream<Object> gpuDstream);

    public void cartesian(Map<DataType, JavaPairDStream<UUID, TaskData>> globalStreamMap,JavaDStream<Object> gpuDstream){
    	JavaPairDStream<UUID, TaskData> aDStream;
    	
    	JavaPairDStream<String, String> cartes = aDStream.transformWithToPair(gpuDstream, 
    			new Function3<JavaPairRDD<String, String>, JavaRDD<String>, Time, JavaPairRDD<String, String>>() {
    		@Override
    		public JavaPairRDD<String, String> call(JavaRDD<String> rddA, JavaRDD<String> rddB, Time v3) throws Exception {
    			JavaPairRDD<String, String> res = rddA.cartesian(rddB);
    			return res;
    		}
    	});
    }*/
    
    
    /**
     * Get input ports of the stream.
     *
     * @return A list of ports.
     */
    public abstract List<Port> getPorts();
    public static String getServerName(){
    	String nodeName1;
        try {
            nodeName1 = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            nodeName1 = "Unknown host";
            e.printStackTrace();
        }
        return nodeName1;
    }
    
    /**
     * 为了保存tracklet的时候，保存在不同目录，每次进行区分
     * 加入保存后，不能细分到秒，不同的流前后时间不同，目录就变多了，天级别
     * LANG
     * @return
     */
    public String getDate(){
    	SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");//设置日期格式
		String dateString=df.format(new Date());
		return dateString;
    }

    /**
     * The class Port represents an input port of a stream.
     */
    public static final class Port implements Serializable {
        private static final long serialVersionUID = -7567029992452814611L;
        /**
         * Name of the port.
         */
        public final String name;
        /**
         * Input data type of the port.
         */
        public final DataType inputType;

        /**
         * Create a port.
         *
         * @param name Name of the port.
         * @param type Input data type of the port.
         */
        public Port(@Nonnull String name,
                    @Nonnull DataType type) {
            this.name = name;
            this.inputType = type;
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof Port) ? name.equals(((Port) o).name) : super.equals(o);
        }
    }
}
