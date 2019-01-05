package org.cripac.isee.vpe.entities;

public class DataInfo {

	private String taskID;
//	private long startTime;
//	private long endTime;
	private String videoName;
	// tracking reid attr
	private String type;
	private String nodeName;
	private int trackletSize;
	private String trackletId;
	private String gpu;
	private String nodeName_gpu;
	private String toString;
	private int hashCode;
	private String isMultiGpu;
//	private String url;
	private long startEachTime;
	private long endEachTime;
	private long borrowObjectTime;
	private long end_start;
	//线程id
	private long pid;
//	private int isException;
	public String getTaskID() {
		return taskID;
	}
	public void setTaskID(String taskID) {
		this.taskID = taskID;
	}
	
	public String getVideoName() {
		return videoName;
	}
	public void setVideoName(String videoName) {
		this.videoName = videoName;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public long getStartEachTime() {
		return startEachTime;
	}
	public void setStartEachTime(long startEachTime) {
		this.startEachTime = startEachTime;
	}
	public long getEndEachTime() {
		return endEachTime;
	}
	public void setEndEachTime(long endEachTime) {
		this.endEachTime = endEachTime;
	}
	public String getNodeName() {
		return nodeName;
	}
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}
	
	public String getTrackletId() {
		return trackletId;
	}
	public void setTrackletId(String trackletId) {
		this.trackletId = trackletId;
	}
	public String getGpu() {
		return gpu;
	}
	public void setGpu(String gpu) {
		this.gpu = gpu;
	}
	public String getToString() {
		return toString;
	}
	public void setToString(String toString) {
		this.toString = toString;
	}
	public int getHashCode() {
		return hashCode;
	}
	public void setHashCode(int hashCode) {
		this.hashCode = hashCode;
	}
	public String getIsMultiGpu() {
		return isMultiGpu;
	}
	public void setIsMultiGpu(String isMultiGpu) {
		this.isMultiGpu = isMultiGpu;
	}
	public int getTrackletSize() {
		return trackletSize;
	}
	public void setTrackletSize(int trackletSize) {
		this.trackletSize = trackletSize;
	}
	public long getBorrowObjectTime() {
		return borrowObjectTime;
	}
	public void setBorrowObjectTime(long borrowObjectTime) {
		this.borrowObjectTime = borrowObjectTime;
	}
	public String getNodeName_gpu() {
		return nodeName_gpu;
	}
	public void setNodeName_gpu(String nodeName_gpu) {
		this.nodeName_gpu = nodeName_gpu;
	}
	public long getEnd_start() {
		return end_start;
	}
	public void setEnd_start(long end_start) {
		this.end_start = end_start;
	}
	public long getPid() {
		return pid;
	}
	public void setPid(long pid) {
		this.pid = pid;
	}
//	public int getIsException() {
//		return isException;
//	}
//	public void setIsException(int isException) {
//		this.isException = isException;
//	}
	@Override
	public String toString() {
		return "DataInfo [taskID=" + taskID + ", videoName=" + videoName + ", type=" + type + ", nodeName=" + nodeName
				+ ", trackletSize=" + trackletSize + ", trackletId=" + trackletId + ", gpu=" + gpu + ", nodeName_gpu="
				+ nodeName_gpu + ", toString=" + toString + ", hashCode=" + hashCode + ", isMultiGpu=" + isMultiGpu
				+ ", startEachTime=" + startEachTime + ", endEachTime=" + endEachTime + ", borrowObjectTime="
				+ borrowObjectTime + ", end_start=" + end_start + ", pid=" + pid + "]";
	}
	
	
	
	
	
}
