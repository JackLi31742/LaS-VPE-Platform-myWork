package org.cripac.isee.vpe.entities;

import java.util.List;

import org.cripac.isee.vpe.entities.ServerInfo.DevInfo.ProcessesDevInfo;

/**
 * 每一台服务器的硬件信息
 * @author LANG
 *
 */
public class ServerInfo {

	//主机名
	public String nodeName;
	public String ip;
	
	// 已使用的内存
	public long usedMem;
	 // 最大可使用内存
	public long jvmMaxMem;
	 // 可使用内存 
	public long jvmTotalMem;
	// 总的物理内存
	public long physicTotalMem;
	//jvm最近的CPU使用率
	public int procCpuLoad;
	//整个系统的最近CPU使用率
	public int sysCpuLoad;
	//物理CPU个数 
	public int cpuNum;
	//每个物理CPU中core的个数(即核数) 
	public int cpuCore;
	//逻辑CPU的个数 
	public int cpuVirtualNum;
	 //gpu个数
	public int deviceCount;
	
	//gpu 编号list
	public List<Integer> devNumList;
	//gpu 正在运行的程序使用的gpu编号list,一台服务器上总的
	public List<Integer> processNumAllList;
	//一台服务器上所有的正在运行的gpu 程序信息
	public List<ProcessesDevInfo> processAllList;
	
	/**
	 * 硬件gpu的信息
	 */
	public List<DevInfo> devInfosList;
	
	/**
	 * DevInfo是一台服务器里的gpu，可能有多个
	 * @author LANG
	 *
	 */
    public static class DevInfo {
    	//gpu编号
    	public int index;
    	public int fanSpeed;
        //GPU使用率
    	public int utilRate;
    	public long usedMem;
    	public long totalMem;
    	public int temp;
    	public int slowDownTemp;
    	public int shutdownTemp;
    	public int powerUsage;
    	public int powerLimit;
    	
        //正在运行的gpu的程序个数
    	public int infoCount;
    	//每个gpu 正在运行的程序使用的gpu编号list
    	public List<Integer> processNumList;
    	
    	public List<ProcessesDevInfo> processesDevInfosList;
    	/**
    	 * ProcessesDevInfo是每个gpu上的正在运行的程序
    	 */
    	
        public static class ProcessesDevInfo {
        	public int index;
        	//正在使用的gpu的程序的pid
        	public int pid;
        	//正在使用的gpu的程序的pid 对应的 内存
        	public long usedGpuMemory;
        }
    }
    
    


}
