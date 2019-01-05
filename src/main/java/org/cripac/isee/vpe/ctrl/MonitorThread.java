
package org.cripac.isee.vpe.ctrl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.cripac.isee.util.WebToolUtils;
import org.cripac.isee.vpe.entities.ClusterInfo;
import org.cripac.isee.vpe.entities.ClusterInfo.ApplicationInfos;
import org.cripac.isee.vpe.entities.ClusterInfo.Nodes;
import org.cripac.isee.vpe.entities.ContainerInfos;
import org.cripac.isee.vpe.entities.Report;
import org.cripac.isee.vpe.entities.ServerInfo;
import org.cripac.isee.vpe.entities.ServerInfo.DevInfo;
import org.cripac.isee.vpe.entities.ServerInfo.DevInfo.ProcessesDevInfo;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.logging.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.management.OperatingSystemMXBean;

public class MonitorThread extends Thread {

    public static final String REPORT_TOPIC = "monitor-desc-";
    
    private Logger logger;
    private KafkaProducer<String, String> reportProducer;
    private Runtime runtime = Runtime.getRuntime();
    private OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    private YarnClient yarnClient;
    private YarnConfiguration conf;
    private SystemPropertyCenter propCenter;
    private Type serverInfosMapType ;
    private Type containerInfosMapType ;
//    private LinuxContainerExecutor linuxContainerExecutor;
//    private String masterName="rtask-nod8";
    private String masterName="cpu-master-nod";
    
    private String appName_nodeName;
    /**
     * native 方法
     * LANG
     */
    private native int getInfoCount(int index);
    
    private native int getPid(int index,int num);
    
    private native long getUsedGpuMemory(int index,int num);
    
    
    private native int initNVML();

    //gpu个数
    private native int getDeviceCount();
    
    private native int getFanSpeed(int index);

    //GPU使用率
    private native int getUtilizationRate(int index);

    private native long getFreeMemory(int index);

    private native long getTotalMemory(int index);

    private native long getUsedMemory(int index);

    private native int getTemperature(int index);

    private native int getSlowDownTemperatureThreshold(int index);

    private native int getShutdownTemperatureThreshold(int index);

    private native int getPowerLimit(int index);

    private native int getPowerUsage(int index);

    
	public MonitorThread(Logger logger, SystemPropertyCenter propCenter) {
        this.logger = logger;
        this.reportProducer = new KafkaProducer<>(propCenter.getKafkaProducerProp(true));
        this.propCenter=propCenter;
        this.yarnClient = getYarnClient();
//        linuxContainerExecutor=new LinuxContainerExecutor();
//        linuxContainerExecutor.setConf(conf);
//        linuxContainerExecutor.init();
        
        this.serverInfosMapType = new TypeToken<Map <String,ServerInfo>>() {}.getType(); 
        this.containerInfosMapType = new TypeToken<Map <String,ContainerInfos>>() {}.getType(); 
        								//Java虚拟机的可用的处理器数量						
        logger.info("Running with " + osBean.getAvailableProcessors() + " processors");
//        	，操作系统的架构是： " + osBean.getArch() + " ");
        KafkaHelper.createTopic(propCenter.zkConn, propCenter.zkSessionTimeoutMs, propCenter.zkConnectionTimeoutMS,
                REPORT_TOPIC+getServerName(),
                propCenter.kafkaNumPartitions, propCenter.kafkaReplFactor);
        
        KafkaHelper.createTopic(propCenter.zkConn, propCenter.zkSessionTimeoutMs, propCenter.zkConnectionTimeoutMS,
        		REPORT_TOPIC+"cluster",
        		propCenter.kafkaNumPartitions, propCenter.kafkaReplFactor);
        
        KafkaHelper.createTopic(propCenter.zkConn, propCenter.zkSessionTimeoutMs, propCenter.zkConnectionTimeoutMS,
        		REPORT_TOPIC+this.appName_nodeName,
        		propCenter.kafkaNumPartitions, propCenter.kafkaReplFactor);
        
    }
    
    
    public MonitorThread(){
    	
    }
    
	/**
	 * 加载动态链接库
	 */
    static {
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(MonitorThread.class);
        try {
//            System.setProperty("java.library.path", System.getProperty("java.library.path")+ ":/home/vpetest.cripac/testgpu/LaS-VPE-Platform/lib/x64"); 
            logger.info("Loading native libraries for MonitorThread from "+ System.getProperty("java.library.path") );
            System.loadLibrary("CudaMonitor4j");
//            System.load("/home/jun.li/monitor.test/src/native/CudaMonitor4j/Release/libCudaMonitor4j.so");
            logger.info("Native libraries for MonitorThread successfully loaded!");
        } catch (Throwable t) {
            logger.error("Failed to load native library for MonitorThread", t);
            throw t;
        }
    }

	@Override
    public void run() {
    	Report report =initReport();
    	ServerInfo serverInfo=getServerInfoReport(report);
        //noinspection InfiniteLoopStatement
//        int count=0;
        while (true) {
        	getDevInfo(report,serverInfo);
        	try {
//				appReport(report);
				getClusterReport(report,serverInfo);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
//        	getAppInfoByNode(report,serverInfo);
        	
        	/**
        	 * 测试gpu，先把监控关闭
        	 */
//        	logger.info(new Gson().toJson(report));
        	
           
            
            this.reportProducer.send(new ProducerRecord<>(REPORT_TOPIC+getServerName(), REPORT_TOPIC+getServerName(), new Gson().toJson(report.serverInfosMap,this.serverInfosMapType)));
            this.reportProducer.send(new ProducerRecord<>(REPORT_TOPIC+"cluster", REPORT_TOPIC+"cluster", new Gson().toJson(report.clusterInfo)));
            this.reportProducer.send(new ProducerRecord<>(REPORT_TOPIC+this.appName_nodeName, REPORT_TOPIC+this.appName_nodeName, new Gson().toJson(report.containerInfosMap,this.containerInfosMapType)));
            
            clearList(report,serverInfo);
            try {
                sleep(50000);
            } catch (InterruptedException ignored) {
            	ignored.printStackTrace();
            }
//            count++;
//            if (count>1) {
//            	report.processNumList.clear();
//            }
        }
    }

	public void clearList(Report report,ServerInfo serverInfo){
		for (int i = 0; i < serverInfo.deviceCount; ++i) {
        	DevInfo info = serverInfo.devInfosList.get(i);
        	List<Integer> processNumList=info.processNumList;
        	if (processNumList!=null) {
				
        		processNumList.clear();
			}
        }
		List<Integer> processNumAllList=serverInfo.processNumAllList;
		List<ProcessesDevInfo> processAllList=serverInfo.processAllList;
		if (processNumAllList!=null) {
			
			processNumAllList.clear();
		}
		if (processAllList!=null) {
			
			processAllList.clear();
		}
	}
	

    
    public Report initReport(){
    	Report report = new Report();
    	
    	report.serverInfosMap=new HashMap<>();
    	String nodeName=getServerName();
    	ServerInfo serverInfo=new ServerInfo();
    	//put之后才有值，同时可以再put之后再给其中的对象赋值，是可以拿到的
    	report.serverInfosMap.put(nodeName, serverInfo);
    	report.clusterInfo=new ClusterInfo();
    	
    	report.containerInfosMap=new HashMap<>();
    	return report;
    }
    
    public ServerInfo getServerInfoReport(Report report){
    	
    	Map <String,ServerInfo> serverInfosMap=report.serverInfosMap;
    	ServerInfo serverInfo=null;
    	for (Entry<String, ServerInfo> entry : serverInfosMap.entrySet()) {
			serverInfo=entry.getValue();
		}
    	serverInfo.nodeName = getServerName();
    	serverInfo.ip=getIp();
        
        logger.info("hostname："+serverInfo.nodeName+",ip:"+serverInfo.ip);

        serverInfo.cpuNum=getCpuNum();
        serverInfo.cpuCore=getCpuCore();
        serverInfo.jvmMaxMem = runtime.maxMemory() / (1024 * 1024);
    	serverInfo.jvmTotalMem = runtime.totalMemory() / (1024 * 1024);
    	serverInfo.physicTotalMem = osBean.getTotalPhysicalMemorySize() / (1024 * 1024);
    	serverInfo.cpuVirtualNum=getCpuVirtualNum();
    	
        int nvmlInitRet = initNVML();
        if (nvmlInitRet == 0) {
        	serverInfo.deviceCount = getDeviceCount();
        	if (serverInfo!=null) {
				
        		logger.info("Running with " + serverInfo.deviceCount + " GPUs.");
			}
        } else {
        	serverInfo.deviceCount = 0;
            logger.info("Cannot initialize NVML: " + nvmlInitRet);
        }
        
        serverInfo.devInfosList = new ArrayList<>();
        serverInfo.devNumList=new ArrayList<>();
        
        
        for (int i = 0; i < serverInfo.deviceCount; ++i) {
        	serverInfo.devInfosList.add(new DevInfo());
            serverInfo.devNumList.add(i);
        }

//        logger.debug("Starting monitoring gpu!");
        return serverInfo;
    }
    
    
    public void getDevInfo(Report report,ServerInfo serverInfo){
    	
    	   											// 剩余内存
    	serverInfo.usedMem = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
    	
        
//        logger.info("Memory consumption: "
//                + serverInfo.usedMem + "/"
//                + serverInfo.jvmMaxMem + "/"
//                + serverInfo.jvmTotalMem + "/"
//                + serverInfo.physicTotalMem + "M");

        serverInfo.procCpuLoad = (int) (osBean.getProcessCpuLoad() * 100);
        serverInfo.sysCpuLoad = (int) (osBean.getSystemCpuLoad() * 100);
//        logger.info("CPU load: " + serverInfo.procCpuLoad + "/" + serverInfo.sysCpuLoad + "%");

//        StringBuilder stringBuilder = new StringBuilder("GPU Usage:");
        serverInfo.processNumAllList=new ArrayList<>();
        serverInfo.processAllList=new ArrayList<>();
        for (int i = 0; i < serverInfo.deviceCount; ++i) {
            DevInfo devInfo = serverInfo.devInfosList.get(i);
            devInfo.index=i;
            devInfo.fanSpeed = getFanSpeed(i);
            devInfo.utilRate = getUtilizationRate(i);
            devInfo.usedMem = getUsedMemory(i);
            devInfo.totalMem = getTotalMemory(i);
            devInfo.temp = getTemperature(i);
            devInfo.slowDownTemp = getSlowDownTemperatureThreshold(i);
            devInfo.shutdownTemp = getShutdownTemperatureThreshold(i);
            devInfo.powerUsage = getPowerUsage(i);
            devInfo.powerLimit = getPowerLimit(i);
            devInfo.infoCount=getInfoCount(i);
            devInfo.processNumList=new ArrayList<>();
            devInfo.processesDevInfosList=new ArrayList<>();
//            if (count==0) {
				
//            	for (int j = 0; j < devInfo.infoCount; j++) {
//            		devInfo.processesDevInfosList.add(new ProcessesDevInfo());
//            		devInfo.processNumList.add(i);
//            	}
            	for (int j = 0; j < devInfo.infoCount; j++) {
            		
					ProcessesDevInfo processesDevInfo = new ProcessesDevInfo();
//							devInfo.processesDevInfosList.get(j);
					processesDevInfo.usedGpuMemory = getUsedGpuMemory(i,j)/ (1024 * 1024);
					processesDevInfo.pid = getPid(i,j);
					processesDevInfo.index=i;
					devInfo.processesDevInfosList.add(processesDevInfo);
					serverInfo.processAllList.add(processesDevInfo);
					devInfo.processNumList.add(i);
            	}
//			}
            	serverInfo.processNumAllList.addAll(devInfo.processNumList);	
//            stringBuilder.append("\n|Index\t|Fan\t|Util\t|Mem(MB)\t|Temp(C)\t|Pow\t|infoCount");
//            stringBuilder.append("\n|").append(devInfo.index)
//                    .append("\t|")
//                    .append(devInfo.fanSpeed)
//                    .append("\t|")
//                    .append(String.format("%3d", devInfo.utilRate)).append('%')
//                    .append("\t|")
//                    .append(String.format("%5d", devInfo.usedMem / (1024 * 1024)))
//                    .append("/").append(String.format("%5d", devInfo.totalMem / (1024 * 1024)))
//                    .append("\t|")
//                    .append(devInfo.temp).append("/").append(devInfo.slowDownTemp).append("/").append(devInfo.shutdownTemp)
//                    .append("\t|")
//                    .append(devInfo.powerUsage).append("/").append(devInfo.powerLimit)
//            		.append("\t|")
//            		.append(devInfo.infoCount);
        }
        
//        logger.info(stringBuilder.toString());
        
        
//        return report;
    }
    
    public String getServerName(){
    	String nodeName1;
        try {
            nodeName1 = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            nodeName1 = "Unknown host";
        }
        return nodeName1;
    }
    
    public String getIp(){
    	String ipString;
        try {
        	ipString=WebToolUtils.getLocalIP();
		} catch (Exception e) {
			// TODO: handle exception
			ipString="Unknown ip";
		}
        return ipString;
    }
    
	public int getCpuNum() {
		final String[] command = {"/bin/sh","-c","cat /proc/cpuinfo | grep 'physical id'| sort | uniq | wc -l"};
		String message = getCommandResult(command).get(0);
		String result = "";
		result = message.split(" ")[0].trim();
//		System.out.println("cpu的数量是："+result);
		return Integer.parseInt(result);
	}
    
    
    
    public int getCpuCore() {
    	final String[] command={"/bin/sh","-c","/usr/bin/cat /proc/cpuinfo | grep 'cpu cores' | uniq"};
    	String message =getCommandResult(command).get(0);
    	String result="";
    	if (message.contains(":")) {
    		result=message.split(":")[1].trim();
		}else if(message.contains("：")){
			result=message.split("：")[1].trim();
		}
    	return Integer.parseInt(result);
    }
    
    public int getCpuVirtualNum() {
    	final String[] command={"/bin/sh","-c","/usr/bin/cat /proc/cpuinfo | grep 'processor' | wc -l"};
    	String message =getCommandResult(command).get(0);
    	String result = "";
		result = message.split(" ")[0].trim();
		return Integer.parseInt(result);
    }
    public List<String> getCommandResult(String[] command) {
    	Process process = null;
    	List<String> resultList=new ArrayList<>();
		try {
			process = runtime.exec(command);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		if (process!=null) {
			
			InputStream in=process.getInputStream();
			InputStreamReader inputStreamReader=new InputStreamReader(in);
	    	BufferedReader bufReader = new BufferedReader(inputStreamReader);
	//    	StringBuffer sb = new StringBuffer();
	    	String line = "";  
	    	try {
				while((line = bufReader.readLine()) != null) {
	//				System.out.println("测试：");
	//				System.out.println("getCommandResult:"+line);
	//			    sb.append(line).append("\n");
				    resultList.add(line);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				try {
					if (bufReader!=null) {
						
						bufReader.close();
						bufReader=null;
					}if (inputStreamReader!=null) {
						
						inputStreamReader.close();
						inputStreamReader=null;
					}if (in!=null) {
						
						in.close();
						in=null;
					}
					if (process!=null) {
						
						if(process.isAlive()){
							process.destroy();
							process=null;
						}
					}
					runtime.gc();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}else {
			System.out.println("process 是空");
		}
    	return resultList;
    }
    
    public List<Integer> getContainerPid(String ContainerId) {
    	final String[] command={"/bin/sh","-c","ps -ef |grep "+ContainerId};
    	List<String> resultList =getCommandResult(command);
    	List<Integer> pidList=new ArrayList<>();
    	for (int i = 0; i < resultList.size(); i++) {
//    		System.out.println("line:"+resultList.get(i));
    		String result =resultList.get(i).split(" ")[1].trim();
//    		System.out.println("result :"+result);
    		if (result!=null) {
    			if(!(result.equals(""))){
    				if (!(result.equals(System.getProperty("line.separator")))) {
						
    					try {
//    						System.out.println("result try:"+result);
    						pidList.add(Integer.parseInt(result));
    					} catch (Exception e) {
    						// TODO: handle exception
//						System.out.println("getContainerPid异常：");
    						e.printStackTrace();
//						pidList.add(0);
    					}
					}
    			}
			}
		}
		return pidList;
    }
    
    public void getClusterReport(Report report,ServerInfo serverInfo)throws YarnException, IOException{
		
		getNodes(report,serverInfo);
		appReport(report,serverInfo);
	}
    
    /**
	 * 得到yarn上的application的cpu和内存
	 * LANG
	 * @throws YarnException
	 * @throws IOException
	 */
	public void appReport(Report report,ServerInfo serverInfo) throws YarnException, IOException {
// 		ApplicationResourceUsageReport applicationResourceUsageReport=new ApplicationReportPBImpl().getApplicationResourceUsageReport();
		
		List<ApplicationReport> list = yarnClient.getApplications(EnumSet.of(YarnApplicationState.RUNNING));
		if (list != null) {
			if (list.size() > 0) {
				List<ApplicationInfos> applicationInfosList=new ArrayList<>();
				report.clusterInfo.applicationInfosList=applicationInfosList;
//				report.clusterInfo.applicationInfosList=new ArrayList<>();
//				report.applicationInfos = new Report.ApplicationInfos[list.size()];
//				for (int i = 0; i < list.size(); i++) {
////					report.applicationInfos[i]=new Report.ApplicationInfos();
//					report.clusterInfo.applicationInfosList.add(new ApplicationInfos());
//				}
				
				//保存所有的container
				List<ContainerInfos> contarinerInfosAllList = new ArrayList<>();
				
				List<String> containerIdAllList = new ArrayList<>();
				//保存每个节点上的container
//				List<ContainerInfos> contarinerInfosList = new ArrayList<>();
				for (int i = 0; i < list.size(); i++) {
					ApplicationReport applicationReport=list.get(i);
					ApplicationResourceUsageReport applicationResourceUsageReport = applicationReport.getApplicationResourceUsageReport();
					ApplicationInfos applicationInfo = new ApplicationInfos();
//					report.clusterInfo.applicationInfosList.get(i);
					report.clusterInfo.applicationInfosList.add(applicationInfo);
					//MB
//					long memory = applicationResourceUsageReport.getMemorySeconds();
//					long vcore = applicationResourceUsageReport.getVcoreSeconds();
//					long preemptedMemory=applicationResourceUsageReport.getPreemptedMemorySeconds();
//					long preemptedVcore=applicationResourceUsageReport.getPreemptedVcoreSeconds();
					//需要的
					Resource neededResource = applicationResourceUsageReport.getNeededResources();
					//使用的
					Resource usedResource = applicationResourceUsageReport.getUsedResources();
					//保留的
					Resource reservedResource = applicationResourceUsageReport.getReservedResources();
					
					ApplicationId applicationId=applicationReport.getApplicationId();
					applicationInfo.applicationId=applicationId+"";
					applicationInfo.appName=applicationReport.getName();
					applicationInfo.neededResourceMemory=neededResource.getMemory();
					applicationInfo.neededResourceVcore=neededResource.getVirtualCores();
					applicationInfo.usedResourceMemory=usedResource.getMemory();
					applicationInfo.usedResourceVcore=usedResource.getVirtualCores();
					applicationInfo.reservedResourceMemory=reservedResource.getMemory();
					applicationInfo.reservedResourceVcore=reservedResource.getVirtualCores();
					
//					System.out.println("ApplicationId:"+applicationInfo.applicationId
//							+",neededResourceMemory:"+applicationInfo.neededResourceMemory+",neededResourceVcore:"+applicationInfo.neededResourceVcore
//							+",usedResourceMemory:"+applicationInfo.usedResourceMemory+",usedResourceVcore:"+applicationInfo.usedResourceVcore
//							+",reservedResourceMemory:"+applicationInfo.reservedResourceMemory+",reservedResourceVcore:"+applicationInfo.reservedResourceVcore);
					
					//container
					getContainers(report,applicationInfo, applicationId,serverInfo,contarinerInfosAllList);
				}
				for (int i = 0; i < contarinerInfosAllList.size(); i++) {
					ContainerInfos containerInfos =contarinerInfosAllList.get(i);
//					if (containerInfos.nodeName.equals(getServerName())) {
//						contarinerInfosList.add(containerInfos);
//					}
					containerIdAllList.add(containerInfos.containerId);
					if (containerInfos.pid!=0) {
//						this.containerId=containerInfos.containerId;
						this.appName_nodeName=containerInfos.appName+"_"+containerInfos.nodeName;
						report.containerInfosMap.put(containerInfos.containerId, containerInfos);
					}
				}
				
			}
		}
	}
	
	
	
	
	/**
	 * jvm 方式，先不测试
	 * LANG
	 * @param report
	 * @param serverInfo
	 * @throws IOException 
	 * @throws YarnException 
	 */
//	public void getAppInfoByNode(Report report,ServerInfo serverInfo){
//		List<ApplicationInfos> applicationInfosList=report.clusterInfo.applicationInfosList;
////		report.clusterInfo.applicationInfosList=applicationInfosList;
//		if (applicationInfosList!=null) {
//			if (applicationInfosList.size()>0) {
//				
//				for (int i = 0; i < applicationInfosList.size(); i++) {
//					ApplicationInfos applicationInfo = applicationInfosList.get(i);
//					applicationInfo.eachAppNodeMap=new HashMap<>();
//					EachAppNode eachAppNode=new EachAppNode();
//					String nodeName=getServerName();
//					applicationInfo.eachAppNodeMap.put(nodeName, eachAppNode);
//					eachAppNode.pid=getPID();
//					eachAppNode.ip=serverInfo.ip;
//					eachAppNode.nodeName=serverInfo.nodeName;
//					eachAppNode.indexList=new ArrayList<>();
//					eachAppNode.usedGpuMemoryList=new ArrayList<>();
//					List<ProcessesDevInfo> processAllList=serverInfo.processAllList;
//					if (processAllList!=null) {
//						if (processAllList.size()>0) {
//							
//							for (int j = 0; j < processAllList.size(); j++) {
//								ProcessesDevInfo processesDevInfo=processAllList.get(j);
//								if (processesDevInfo.pid==eachAppNode.pid) {
//									eachAppNode.indexList.add(processesDevInfo.index);
//									eachAppNode.usedGpuMemoryList.add(processesDevInfo.usedGpuMemory);
//								}
//							}
//						}
//					}
//				}
//			}
//		}
//	}
	
	
	public void getNodes(Report report,ServerInfo serverInfo) throws YarnException, IOException {
		List<NodeReport> nodeList=yarnClient.getNodeReports(NodeState.RUNNING);
		List<Nodes> nodeInfosList=new ArrayList<>();
		report.clusterInfo.nodeInfosList=nodeInfosList;
//		for (int i = 0; i < nodeList.size(); i++) {
//			nodeInfosList.add(new Nodes());
//		}
		for (int i = 0; i < nodeList.size(); i++) {
			Nodes nodes=new Nodes();
			nodeInfosList.add(nodes);
			
			NodeReport nodeReport=nodeList.get(i);
			nodes.name=nodeReport.getNodeId().getHost();
			
			Resource capResource=nodeReport.getCapability();
			nodes.capabilityCpu=capResource.getVirtualCores();
			nodes.capabilityMemory=capResource.getMemory();
			
			Resource usedResource=nodeReport.getUsed();
			nodes.usedCpu=usedResource.getVirtualCores();
			nodes.usedMemory=usedResource.getMemory();
			
		}
	}
	
	public void getContainers(Report report, ApplicationInfos applicationInfo,
			ApplicationId applicationId,ServerInfo serverInfo,List<ContainerInfos> contarinerInfosAllList) throws YarnException, IOException {
		List<ApplicationAttemptReport> applicationAttemptReportsList = yarnClient.getApplicationAttempts(applicationId);
		
//		applicationInfo.containerInfosList = contarinerInfosList;
		applicationInfo.containerIdList=new ArrayList<>();
		if (applicationAttemptReportsList != null) {
			if (applicationAttemptReportsList.size() > 0) {
//				System.out.println(applicationId+"的attempt的大小是：" + applicationAttemptReportsList.size());
				for (int i = 0; i < applicationAttemptReportsList.size(); i++) {
					ApplicationAttemptReport applicationAttemptReport = applicationAttemptReportsList.get(i);
					ApplicationAttemptId applicationAttemptId =applicationAttemptReport.getApplicationAttemptId();
					List<ContainerReport> containerReportsList = yarnClient.getContainers(applicationAttemptId);
					if (containerReportsList != null) {
						if (containerReportsList.size() > 0) {
							for (int j = 0; j < containerReportsList.size(); j++) {
								ContainerReport containerReport = containerReportsList.get(j);
								ContainerId containerId = containerReport.getContainerId();
								String containerIdStr = ConverterUtils.toString(containerId);
								ContainerState containerState = containerReport.getContainerState();
								Resource allocatedResource = containerReport.getAllocatedResource();
								String hostName=containerReport.getAssignedNode().getHost();
//								String pid=linuxContainerExecutor.getProcessId(containerId);
								
								ContainerInfos containerInfos = new ContainerInfos();
								containerInfos.applicationId=applicationId+"";
								containerInfos.applicationAttemptId=applicationAttemptId+"";
								containerInfos.containerId = containerIdStr;
								containerInfos.allocatedCpu = allocatedResource.getVirtualCores();
								containerInfos.allocatedMemory = allocatedResource.getMemory();
								containerInfos.state = containerState + "";
								
								containerInfos.nodeName=hostName;
								containerInfos.appName=applicationInfo.appName;
								List<Integer> pidList=getContainerPid(containerIdStr);
//								List<Integer> pidList=getContainerPid(applicationId+"");
								for (int k = 0; k < pidList.size(); k++) {
									
									if (pidList.get(k)==getPID()) {
										containerInfos.pid=pidList.get(k);
//										System.out.println(containerIdStr+":"+hostName+",pid是:"+containerInfos.pid);
									}
								}

								applicationInfo.containerIdList.add(containerIdStr);
//								containerInfos.eachAppNode=new EachAppNode();
////								if (pid!=null) {
////									
////									containerInfos.eachAppNode.pid=Integer.parseInt(pid);
////								}
//								containerInfos.eachAppNode.pid=getPID();
//								System.out.println("containerInfos.eachAppNode.pid："+containerInfos.eachAppNode.pid);
////								containerInfos.eachAppNode.ip=serverInfo.ip;
//								containerInfos.eachAppNode.nodeName=hostName;
								List<ProcessesDevInfo> processAllList=serverInfo.processAllList;
								containerInfos.indexList=new ArrayList<>();
								containerInfos.usedGpuMemoryList=new ArrayList<>();
								if (processAllList!=null) {
									if (processAllList.size()>0) {
										
										for (int k = 0; k < processAllList.size(); k++) {
											ProcessesDevInfo processesDevInfo=processAllList.get(k);
											if (processesDevInfo.pid==containerInfos.pid) {
												containerInfos.indexList.add(processesDevInfo.index);
												containerInfos.usedGpuMemoryList.add(processesDevInfo.usedGpuMemory);
//												applicationInfo.usedResourceGpuMemory+=processesDevInfo.usedGpuMemory;
											}
										}
									}
								}
								contarinerInfosAllList.add(containerInfos);
							}
						}
					}
				}
				
				
			}
		}
	}
	
	
	/**
	 * 得到yarn
	 * LANG
	 * @return
	 */
	public YarnClient getYarnClient(){
		this.conf = new YarnConfiguration();
//		conf.set("fs.defaultFS", "hdfs://rtask-nod8:8020");
		this.conf.set("yarn.resourcemanager.scheduler.address", masterName+":8030");
		this.conf.set(YarnConfiguration.NM_LOCAL_DIRS, "/home/vpetest.cripac/softwares/hadoop_dirs/tmp/nm-local-dir");
		this.conf.set(YarnConfiguration.NM_LOG_DIRS, "/home/vpetest.cripac/softwares/hadoop-2.7.4/logs/userlogs/");
		String hadoopHome = System.getenv("HADOOP_HOME");
		this.conf.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
		this.conf.addResource(new Path(hadoopHome + "/etc/hadoop/yarn-site.xml"));
//		LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
//	    dirsHandler.init(conf);
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(this.conf);
		yarnClient.start();
		return yarnClient;
	}
	
	/**
	 * 将该方法放在spark或者hadoop的分布式程序中，可以得到该程序在每台节点上的pid
	 * 但是目前的位置就可以得到container的pid，但是赋值有问题
	 */
	public int getPID() {
	    String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
	    if (processName != null && processName.length() > 0) {
	        try {
	            return Integer.parseInt(processName.split("@")[0]);
	        }
	        catch (Exception e) {
	            return 0;
	        }
	    }

	    return 0;
	}
}
