package org.cripac.isee.vpe.entities;

import java.util.List;


/**
 * 从yarn上读取
 */
public class ClusterInfo {

	public List<Nodes> nodeInfosList;
	public List<ApplicationInfos> applicationInfosList;
	//保存所有的containerId
	public List<String> containerIdAllList;
	
	/**
	 * 集群节点的信息
	 * @author LANG
	 *
	 */
	public static class Nodes{
		//名字
		public String name;
		//容量cpu
		public int capabilityCpu;
		//容量内存
		public int capabilityMemory;
		//已使用的cpu
		public int usedCpu;
		//已使用的内存
		public int usedMemory;
	}
	/**
	 * 每个application的信息
	 */
	public static class ApplicationInfos{
    	public String applicationId;
    	public String appName;
    	public int neededResourceMemory;
    	public int neededResourceVcore;
    	public int usedResourceMemory;
    	public int usedResourceVcore;
    	public int reservedResourceMemory;
    	public int reservedResourceVcore;
    	//cpu以及内存的信息，通过yarn去拿container的
    	//供reportAll使用
    	public List<ContainerInfos> containerInfosList;
    	
    	//使用containerId匹配container，这里是每一个app的container
    	public List<String> containerIdList;
    	/**
    	 * 这个是为了拿到gpu的信息，container方式下可不使用
    	 */
//    	public Map<String,EachAppNode> eachAppNodeMap;
    	
    	//每一个app使用的总的显存
//    	public long usedResourceGpuMemory;
    	
    	
    }
	
	
	
	
	/**
	 * 每台节点的信息，这些信息的数量是集群pc的数量，但由于是分布式，每个只能得到一台节点的，主要是为了得到gpu信息
	 * jvm方式和ContarinerInfos同级
	 */
	/*public static class EachAppNode{
		public String nodeName;
		public String ip;
		
		public int pid;
		//gpu 编号
		public List<Integer> indexList;
		public List<Long> usedGpuMemoryList;
	}*/


}
