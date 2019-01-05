package org.cripac.isee.vpe.entities;

import java.util.Map;

/**
 * Report是一台服务器级，
 * @author LANG
 *
 */
public class Report {

	public ClusterInfo clusterInfo;
	/**
	 * key 是hostname，value是每个节点的信息
	 */
	public Map <String,ServerInfo> serverInfosMap;
	/**
	 * key 是containerId，value是每个节点上container的信息
	 * 用来保存发送时的，接收后，所有的将保存到ClusterInfo
	 */
	
	public Map <String,ContainerInfos> containerInfosMap;
	

}
