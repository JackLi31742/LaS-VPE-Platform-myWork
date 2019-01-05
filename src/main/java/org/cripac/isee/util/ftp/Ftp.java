package org.cripac.isee.util.ftp;

public class Ftp {
	private String ipAddr;//ip地址  
	  
    private Integer port;//端口号  
  
    private String userName;//用户名  
  
    private String pwd;//密码  
  
  
    private String sysFile;//项目中内容所要上传的文件夹地址  


	public String getIpAddr() {
		return ipAddr;
	}


	public void setIpAddr(String ipAddr) {
		this.ipAddr = ipAddr;
	}


	public Integer getPort() {
		return port;
	}


	public void setPort(Integer port) {
		this.port = port;
	}


	public String getUserName() {
		return userName;
	}


	public void setUserName(String userName) {
		this.userName = userName;
	}


	public String getPwd() {
		return pwd;
	}


	public void setPwd(String pwd) {
		this.pwd = pwd;
	}


	public String getSysFile() {
		return sysFile;
	}


	public void setSysFile(String sysFile) {
		this.sysFile = sysFile;
	}
  
    
  
  
}  


