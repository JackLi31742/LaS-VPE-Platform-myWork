package org.cripac.isee.vpe.ctrl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.cripac.isee.vpe.entities.Report;
import org.cripac.isee.vpe.entities.ServerInfo;
import org.cripac.isee.vpe.entities.ServerInfo.DevInfo;
import org.cripac.isee.vpe.entities.ServerInfo.DevInfo.ProcessesDevInfo;

import com.google.gson.Gson;

public class GpuManage {
	
	static {
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(GpuManage.class);
        try {
//            System.setProperty("java.library.path", System.getProperty("java.library.path")  
//            		+ ":/home/vpetest.cripac/testgpu/LaS-VPE-Platform/lib/x64"); 
            logger.info("Loading native libraries for GpuManage from "+ System.getProperty("java.library.path"));
            System.loadLibrary("CudaMonitor4j4GpuManage");
//            System.load("/home/vpetest.cripac/testgpu/LaS-VPE-Platform/lib/x64/libCudaMonitor4j.so");
//            System.load("/home/vpetest.cripac/testgpu/LaS-VPE-Platform/lib/x64/libnvidia-ml.so");
            logger.info("Native libraries for GpuManage successfully loaded!");
        } catch (Throwable t) {
            logger.error("Failed to load native library for GpuManage", t);
            throw t;
        }
    }
	
	
	 public Report get() {
	    	Report report =initReport();
	    	ServerInfo serverInfo=getServerInfoReport(report);
	        //noinspection InfiniteLoopStatement
//	        int count=0;
//	        while (true) {
	        	getDevInfo(report,serverInfo);
//	        	try {
////					appReport(report);
////					getClusterReport(report,serverInfo);
//				} catch (Exception e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				} 
//	        	getAppInfoByNode(report,serverInfo);
	        	
	        	/**
	        	 * 测试gpu，先把监控关闭
	        	 */
	        	/*logger.info(new Gson().toJson(report));
	        	KafkaHelper.createTopic(propCenter.zkConn, propCenter.zkSessionTimeoutMs, propCenter.zkConnectionTimeoutMS,
	                    REPORT_TOPIC+getServerName(),
	                    propCenter.kafkaNumPartitions, propCenter.kafkaReplFactor);
	            
	            KafkaHelper.createTopic(propCenter.zkConn, propCenter.zkSessionTimeoutMs, propCenter.zkConnectionTimeoutMS,
	            		REPORT_TOPIC+"cluster",
	            		propCenter.kafkaNumPartitions, propCenter.kafkaReplFactor);
	            
	            KafkaHelper.createTopic(propCenter.zkConn, propCenter.zkSessionTimeoutMs, propCenter.zkConnectionTimeoutMS,
	            		REPORT_TOPIC+this.appName_nodeName,
	            		propCenter.kafkaNumPartitions, propCenter.kafkaReplFactor);
	            */
	            
//	            this.reportProducer.send(new ProducerRecord<>(REPORT_TOPIC+getServerName(), REPORT_TOPIC+getServerName(), new Gson().toJson(report.serverInfosMap,this.serverInfosMapType)));
//	            this.reportProducer.send(new ProducerRecord<>(REPORT_TOPIC+"cluster", REPORT_TOPIC+"cluster", new Gson().toJson(report.clusterInfo)));
//	            this.reportProducer.send(new ProducerRecord<>(REPORT_TOPIC+this.appName_nodeName, REPORT_TOPIC+this.appName_nodeName, new Gson().toJson(report.containerInfosMap,this.containerInfosMapType)));
	            
//	            clearList(report,serverInfo);
//	            try {
//	            } catch (InterruptedException ignored) {
//	            }
//	            count++;
//	            if (count>1) {
//	            	report.processNumList.clear();
//	            }
//	        }
	            return report;
	    }
	
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
	
	 public Report initReport(){
		 	Report report = new Report();
	    	
	    	report.serverInfosMap=new HashMap<>();
	    	String nodeName=getServerName();
	    	ServerInfo serverInfo=new ServerInfo();
	    	//put之后才有值，同时可以再put之后再给其中的对象赋值，是可以拿到的
	    	report.serverInfosMap.put(nodeName, serverInfo);
	    	return report;
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
	 
	 
	 public ServerInfo getServerInfoReport(Report report){
	    	
	    	Map <String,ServerInfo> serverInfosMap=report.serverInfosMap;
	    	ServerInfo serverInfo=null;
	    	for (Entry<String, ServerInfo> entry : serverInfosMap.entrySet()) {
				serverInfo=entry.getValue();
			}
	    	serverInfo.nodeName = getServerName();
	        

	        
	        int nvmlInitRet = initNVML();
	        if (nvmlInitRet == 0) {
	        	serverInfo.deviceCount = getDeviceCount();
	        	if (serverInfo!=null) {
					
	        		System.out.println("GpuManage:"+"Running with " + serverInfo.deviceCount + " GPUs.");
				}
	        } else {
	        	serverInfo.deviceCount = 0;
	        	System.out.println("GpuManage:"+"Cannot initialize NVML: " + nvmlInitRet);
	        }
	        
	        serverInfo.devInfosList = new ArrayList<>();
	        serverInfo.devNumList=new ArrayList<>();
	        
	        
	        for (int i = 0; i < serverInfo.deviceCount; ++i) {
	        	serverInfo.devInfosList.add(new DevInfo());
	            serverInfo.devNumList.add(i);
	        }

	        return serverInfo;
	    }
	 public void getDevInfo(Report report,ServerInfo serverInfo){
	    	

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
	            	for (int j = 0; j < devInfo.infoCount; j++) {
	            		
						ProcessesDevInfo processesDevInfo = new ProcessesDevInfo();
						processesDevInfo.usedGpuMemory = getUsedGpuMemory(i,j)/ (1024 * 1024);
						processesDevInfo.pid = getPid(i,j);
						processesDevInfo.index=i;
						devInfo.processesDevInfosList.add(processesDevInfo);
						serverInfo.processAllList.add(processesDevInfo);
						devInfo.processNumList.add(i);
	            	}
	            	serverInfo.processNumAllList.addAll(devInfo.processNumList);	
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
}
