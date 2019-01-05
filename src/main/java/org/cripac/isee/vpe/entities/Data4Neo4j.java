package org.cripac.isee.vpe.entities;


import org.cripac.isee.alg.pedestrian.attr.Attributes;
import org.cripac.isee.alg.pedestrian.reid.Feature;

public class Data4Neo4j {
	private String nodeID; 
	private String dataType; 
	private Feature fea;
	private Attributes attr;
	private String trackletPath;
	private String trackletInfo;
	public String getNodeID() {
		return nodeID;
	}
	public void setNodeID(String nodeID) {
		this.nodeID = nodeID;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public Feature getFea() {
		return fea;
	}
	public void setFea(Feature fea) {
		this.fea = fea;
	}
	public Attributes getAttr() {
		return attr;
	}
	public void setAttr(Attributes attr) {
		this.attr = attr;
	}
	public String getTrackletPath() {
		return trackletPath;
	}
	public void setTrackletPath(String trackletPath) {
		this.trackletPath = trackletPath;
	}
	public String getTrackletInfo() {
		return trackletInfo;
	}
	public void setTrackletInfo(String trackletInfo) {
		this.trackletInfo = trackletInfo;
	}
	
}
