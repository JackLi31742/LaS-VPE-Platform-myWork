package org.cripac.isee.vpe.entities;

import java.util.List;
/**
 * container的信息，分配的资源
 * @author LANG
 *
 */
public class ContainerInfos {

	public String applicationId;
	public String applicationAttemptId;
	public String containerId;
	public String appName;
	public String state;
	//内存
	public int allocatedMemory;
	//cpu
	public int allocatedCpu;
	
//		public EachAppNode eachAppNode;
	
	public String nodeName;
	public int pid;
	//gpu 编号
	public List<Integer> indexList;
	public List<Long> usedGpuMemoryList;
	

}
