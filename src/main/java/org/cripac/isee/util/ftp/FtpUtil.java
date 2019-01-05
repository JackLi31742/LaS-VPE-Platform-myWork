package org.cripac.isee.util.ftp;

import static org.bytedeco.javacpp.opencv_core.CV_8UC3;
import static org.bytedeco.javacpp.opencv_imgcodecs.imencode;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.commons.codec.binary.Base64;  // Add by da.li.
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class FtpUtil {
	private  Logger logger=Logger.getLogger(FtpUtil.class);  
	  
	  
    private static FtpUtil instance =null;
  
    //单线程使用
    private static  FTPClient ftp;  
    private ThreadLocal<FTPClient> ftpClientThreadLocal = new ThreadLocal<FTPClient>(); //线程局部变量  
  
    public static FtpUtil getInstance( ) {
        if (instance == null) {
            synchronized (FtpUtil.class) {
                if (instance == null) {
                    instance = new FtpUtil();  
                }
            }
        }
        return instance;
    }
    
  
    
    /** 
     * 获取ftp链接常量 
     * @return 
     */  
    public  Ftp getFtp(){  
        Ftp f=new Ftp();  
        f.setIpAddr("172.18.33.4");  
        f.setUserName("vpe.cripac");  
        f.setPwd("cripac123");  
        f.setPort(21);  
        f.setSysFile("/data/vpe.cripac/ftp/data/img"+"/");  
        return f;  
    } 
    
  
    /** 
     * 获取ftp连接 
     * @param f 
     * @return 
     * @throws Exception 
     */  
    public  boolean connectFtp(Ftp f) throws Exception{  
        ftp=new FTPClient();  
        boolean flag=false;  
        int reply;  
        if (f.getPort()==null) {  
            ftp.connect(f.getIpAddr(),21);  
        }else{  
            ftp.connect(f.getIpAddr(),f.getPort());  
        }  
        ftp.login(f.getUserName(), f.getPwd());  
        ftp.setFileType(FTPClient.BINARY_FILE_TYPE);  
        reply = ftp.getReplyCode();  
        if (!FTPReply.isPositiveCompletion(reply)) {  
            ftp.disconnect();  
            return flag;  
        }  
      /*  ftp.setDataTimeout(60000);       //设置传输超时时间为60秒 
        ftp.setConnectTimeout(60000);*/  
        ftp.changeWorkingDirectory("/");  
        flag = true;  
        return flag;  
    }  
  
  
  
    /** 
     * 关闭ftp连接 
     */  
    public  void closeFtp(){  
        if (ftp!=null && ftp.isConnected()) {  
            try {  
                ftp.logout();  
                ftp.disconnect();  
            } catch (IOException e) {  
                e.printStackTrace();  
            }  
        }  
    }  
  
    /** 
     * 获取ftp连接（多线程） 
     * @return 
     * @throws Exception 
     */  
    public  FTPClient getFTPClient(Ftp ftpProperties) throws Exception{  
    	FTPClient ftpClient=ftpClientThreadLocal.get();
        if (ftpClient != null && ftpClient.isConnected()) {  
        	System.out.println("ThreadLocal获取连接成功！"+ftpClient.hashCode());
        	boolean b= ftpClient.login(ftpProperties.getUserName(), ftpProperties.getPwd());  
        	System.out.println("ThreadLocal登录ftp："+b);
            return ftpClient;  
        }else {
			
        	ftpClient=new FTPClient();  
        	ftpClient.connect(ftpProperties.getIpAddr(),ftpProperties.getPort());  
        	
        	boolean b= ftpClient.login(ftpProperties.getUserName(), ftpProperties.getPwd());  
        	System.out.println("登录ftp："+b);
        	if (b) {
				
//        	int reply;  
//        	reply = ftpClient.getReplyCode();  
//        	if (!FTPReply.isPositiveCompletion(reply)) {  
//        		ftpClient.disconnect();  
//        		System.out.println("reply一样，断开连接");
//        	}
        		ftpClient.enterLocalActiveMode();    //主动模式
        		// ftpClient.enterLocalPassiveMode(); 被动模式
        		ftpClient.setControlEncoding("UTF-8");
//            ftpClient.setUseEPSVwithIPv4(true);  
        		ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
        		ftpClientThreadLocal.set(ftpClient);  
        		System.out.println("ftpClient获取连接成功！"+ftpClient.hashCode());
        		return ftpClient;  
			}else {
				System.out.println("获取ftp连接失败");
				return null;
			}
		}  
    }  
  
    /** 
     * 断开ftp连接（多线程） 
     * 
     * @throws Exception 
     */  
    public void disconnect(FTPClient ftpClient)  {  
        try {  
        	FTPClient obFtpClient=ftpClientThreadLocal.get();
        	if (obFtpClient==ftpClient) {
				
        		ftpClient.logout();  
        		if (ftpClient.isConnected()) {  
        			ftpClient.disconnect();  
        			ftpClient = null;  
        		}
        		ftpClientThreadLocal.remove();
			}else {
				System.out.println("release的对象和当前线程的对象不一样，release的对象的reply是:"+ftpClient.getReplyCode()+"当前线程的reply是："+obFtpClient.getReplyCode());
			}
        } catch (Exception e) {  
            e.printStackTrace(); 
        }  
    }  
  
  
  
     
  
    /** 
     * InputStream-->byte[] 
     * @throws IOException 
     */  
    public  byte[] readInputStream(InputStream inputStream) throws Exception{  
        byte[] buffer = new byte[1024];  
        int len = -1;  
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();  
        while((len = inputStream.read(buffer)) != -1){  
            outputStream.write(buffer, 0, len);  
        }  
        outputStream.close();  
        inputStream.close();  
        return outputStream.toByteArray();  
    }  
  
    /* 
     * 下载获得文件的二进制流信息 
     * @param key 
     * @return 
     * @throws java.io.IOException 
     */  
    public  byte[] download(String key) throws Exception {  
        return download(key, null);  
    }  
  
    /** 
     * 多线程下载 
     * @param key 
     * @param type 
     * @return 
     */  
   public  byte[] download(String key,String type) {  
       byte[] objFile=null;  
       Ftp f =  getFtp();  
       String cacheFile = "/cache_"+key;  
       if(null == type){  
           //设置目录  
           key=f.getSysFile()+key;  
       }else{  
           //设置目录  
       }  
       FTPClient ftpClient = null;  
       try {  
           ftpClient = getFTPClient(f);  
           if (ftpClient!=null && ftpClient.isConnected()) {  
               try {  
                   ftpClient.changeWorkingDirectory("/");  
                   File localFile = new File(cacheFile);  
                   if(localFile.exists()){  
                       localFile.createNewFile();//创建文件  
                   }  
                   OutputStream outputStream = new FileOutputStream(localFile);  
                   ftpClient.enterLocalPassiveMode();  
                   ftpClient.setUseEPSVwithIPv4(true);  
//                    ftp.retrieveFile("1.jpg", outputStream);  
                   ftpClient.retrieveFile(key, outputStream);  
                   InputStream inputStream = new FileInputStream(localFile);  
                   objFile = readInputStream(inputStream);  
                   outputStream.close();  
                   inputStream.close();  
                   localFile.delete();  
  
               } catch (Exception e) {  
                   logger.error(e);  
                   logger.error("下载过程中出现异常");  
               }  
  
           }else{  
               logger.error("链接失败！");  
           }  
       }catch (Exception e){  
           e.printStackTrace();  
       }finally {  
           try{  
               disconnect(ftpClient);  
           }catch (Exception e){  
               e.printStackTrace();  
           }  
  
       }  
       return objFile;  
   }  
  
    /** 
     * 上传文件 
     * @param key 实为文件名，可以为:upload/20150306/text.txt 
     * @param input 文件流 
     * @return 
     */  
    public  void upload(String ftpFileName, InputStream input,Ftp f,FTPClient ftpClient){  
//        upload(ftpFileName, input, null,f,ftpClient);  
    }  
  
    /** 
     * 多线程上传 
     * @param key 
     * @param localInputStream 
     * @param type 
     * @return 
     */  
    public synchronized boolean upload(InputStream localInputStream,String ftpDir,String ftpFileName,FTPClient ftpClient)  {  
        
  
        try {  
            if (ftpClient!=null && ftpClient.isConnected()) {  
            	
            	boolean dirExist=ftpClient.changeWorkingDirectory(ftpDir);
            	if (dirExist) {
					
            		boolean b=ftpClient.storeFile(ftpFileName,localInputStream);  
            		System.out.println("上传文件："+b);
            		return b;
				}
            }  
        }catch (Exception e){  
            e.printStackTrace();  
        }finally {  
            try{  
                disconnect(ftpClient); 
                if(localInputStream!=null){  
                   localInputStream.close();  
                }  
            }catch (Exception e){  
                e.printStackTrace();  
            }  
        } 
        return false;
    }  
  
  
  
    /** 
     * 删除一个存储在FTP服务器上的文件 
     * @param key 
     */  
    public  void delete(String key){  
        Ftp f =  getFtp();  
        key=f.getSysFile()+key;  
        try{  
            if (connectFtp(f)) {  
                ftp.deleteFile(key);  
            }  
  
        }catch (Exception e){  
            e.printStackTrace();  
        }finally {  
            closeFtp();  
        }  
  
    }  
  
  
    /** 
     * 多线程下载 
     * @param fileName 
     * @return 
     */  
    public  Object[] getDownLoadStream(String fileName){  
  
        Long dataLength = null;  
        byte[] objFile=null;  
        Ftp f =  getFtp();  
        fileName=f.getSysFile()+fileName;  
        FTPClient ftpClient = null;  
        try {  
            ftpClient = getFTPClient(f);  
            if (ftpClient!=null && ftpClient.isConnected()) {  
  
                File localFile = new File("/cache_"+fileName);  
                if(localFile.exists()){  
                    localFile.createNewFile();//创建文件  
                }  
                OutputStream outputStream = new FileOutputStream(localFile);  
                ftpClient.enterLocalPassiveMode();  
                ftpClient.setUseEPSVwithIPv4(true);  
//                    ftp.retrieveFile("1.jpg", outputStream);  
                ftpClient.retrieveFile(fileName, outputStream);  
                InputStream inputStream = new FileInputStream(localFile);  
                objFile = readInputStream(inputStream);  
                dataLength = (long)inputStream.available();  
                outputStream.close();  
                inputStream.close();  
                localFile.delete();  
            }  
        } catch (Exception e) {  
            e.printStackTrace();  
        } finally {  
            try{  
                disconnect(ftpClient);  
            }catch (Exception e){  
                e.printStackTrace();  
            }  
        }  
        Object[] res = new Object[]{objFile,dataLength};  
  
        return res;  
    }  
  
    /** 
     * 获取Ftp下载路径 
     * @param key 
     * @return 
     */  
//    public  String getObjectUrl(String key){  
//        Ftp f =  getFtp();  
//        key=f.getSysFile()+key;  
//        String alpath = "ftp://"+f.getUserNameRead()+":"+f.getPwdRead()+"@"+f.getIdApprRead()+":"+f.getPort() + key;  
//        return alpath;  
//    }  
  
    
    /** 
     * 创建目录(有则切换目录，没有则创建目录) 
     * @param dir 
     * @return 
     */  
    public synchronized boolean createDir(String dir,FTPClient ftpClient){  
//        if(StringExtend.isNullOrEmpty(dir))  
//            return true;  
        String d="";  
        try {  
            //目录编码，解决中文路径问题  
            d = new String(dir.toString().getBytes("UTF8"),"iso-8859-1");  
            //尝试切入目录  
            if(ftpClient.changeWorkingDirectory(d)){  
            	System.out.println("目录已经存在："+d);
                return true;  
            }
            dir = StringExtend.trimStart(dir, "/");  
            dir = StringExtend.trimEnd(dir, "/");  
            String[] arr =  dir.split("/");  
            StringBuffer sbfDir=new StringBuffer();  
            //循环生成子目录  
            for(String s : arr){  
                sbfDir.append("/");  
                sbfDir.append(s);  
                //目录编码，解决中文路径问题  
                d = new String(sbfDir.toString().getBytes("UTF8"),"iso-8859-1");  
                //尝试切入目录  
                if(ftpClient.changeWorkingDirectory(d))  
                    continue;  
                if(!ftpClient.makeDirectory(d)){  
                    System.out.println("ftp创建目录失败："+sbfDir.toString());  
                    return false;  
                }  
                System.out.println("创建ftp目录成功："+sbfDir.toString());  
            }  
            //将目录切换至指定路径  
            return ftpClient.changeWorkingDirectory(d);  
        } catch (Exception e) {  
            e.printStackTrace();  
            return false;  
        }  
    }  
    
    /**
    *  检验指定路径的文件是否存在ftp服务器中
    * @param filePath--指定绝对路径的文件
    * @param user--ftp服务器登陆用户名
    * @param passward--ftp服务器登陆密码
    * @param ip--ftp的IP地址
    * @param port--ftp的端口号
    * @return
    */

    public synchronized boolean isFTPFileExist(String ftpDir,String ftpFileName,FTPClient ftpClient){
            try {
                // 提取绝对地址的目录以及文件名
//                String dir = filePath.substring(0, filePath.lastIndexOf("/"));
//                String file = filePath.substring(filePath.lastIndexOf("/")+1);
                
                // 进入文件所在目录，注意编码格式，以能够正确识别中文目录
               boolean b= ftpClient.changeWorkingDirectory(new String(ftpDir.getBytes("UTF-8"),FTPClient.DEFAULT_CONTROL_ENCODING));
               InputStream is =null;
               if (b) {
            	   is = ftpClient.retrieveFileStream(new String(ftpFileName.getBytes("UTF-8"),FTPClient.DEFAULT_CONTROL_ENCODING));
				
               }
                // 检验文件是否存在
                if(is == null || ftpClient.getReplyCode() == FTPReply.FILE_UNAVAILABLE){
                    return false;
                }
                
                if(is != null){
                    is.close();
                    ftpClient.completePendingCommand();
                    return true; 
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
//            finally{
//                if(ftpClient != null){
//                    try {
//                    	ftpClient.disconnect();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
            return false;
    }

    public synchronized void generateImage(Tracklet tracklet,String ftpDir,String ftpFileName,FTPClient ftpClient){
		int trackLength = tracklet.locationSequence.length;
		for (int ti = 0; ti < trackLength; ++ti) {
			final Tracklet.BoundingBox bbox = tracklet.locationSequence[ti];
			if (bbox.patchData != null) {
				final BytePointer inputPointer = new BytePointer(bbox.patchData);
				final opencv_core.Mat image = new opencv_core.Mat(bbox.height, bbox.width, CV_8UC3, inputPointer);
				final BytePointer outputPointer = new BytePointer();
				imencode(".jpg", image, outputPointer);
				byte[] bytes = new byte[(int) outputPointer.limit()];
				outputPointer.get(bytes);
				ByteArrayInputStream in = new ByteArrayInputStream(bytes);
				upload(in, ftpDir, ftpFileName, ftpClient);
				image.release();
				inputPointer.deallocate();
				outputPointer.deallocate();
				break;
			} 
//			else {
//				System.out.println("bbox.patchData 是空的");
//			}
		}
    }
    
	public static String[] generateImage(@Nonnull String videoURL, @Nonnull String trackletDataPath,@Nonnull String outputDir) throws Exception {
		// FileOperation fo = new FileOperation();
		// fo.createDirectory(outputDir);
		//
		// fo=null;
		String imageFilenameTemp = outputDir + "/" + videoURL + "-";
		// Load file ...
		String trackletDataFilename = trackletDataPath + "/tracklet.data";
		final InputStreamReader dataReader = new InputStreamReader(new FileInputStream(new File(trackletDataFilename)));
		BufferedReader br = new BufferedReader(dataReader);
		// Read line by line.
		JsonParser jParser = new JsonParser();
		String jsonData = null;
		List<String> imgFilenames = new ArrayList<String>();
		while ((jsonData = br.readLine()) != null) {
			JsonObject jObject = jParser.parse(jsonData).getAsJsonObject();
			int w = Integer.parseInt(jObject.get("width").getAsString());
			int h = Integer.parseInt(jObject.get("height").getAsString());
			int idx = Integer.parseInt(jObject.get("idx").getAsString());
			String dataBase64String = jObject.get("data").getAsString();
			if (0 == w * h) {
				continue;
			}
			byte[] data = Base64.decodeBase64(dataBase64String);
			// Generate images.
			final BytePointer inputPointer = new BytePointer(data);
			final opencv_core.Mat image = new opencv_core.Mat(h, w, CV_8UC3, inputPointer);
			final BytePointer outputPointer = new BytePointer();
			imencode(".jpg", image, outputPointer);
			// Output image.
			String filename = imageFilenameTemp + idx + ".jpg";
			imgFilenames.add(filename);
			final byte[] bytes = new byte[(int) outputPointer.limit()];
			outputPointer.get(bytes);
			final FileOutputStream imgOutputStream;
			try {
				imgOutputStream = new FileOutputStream(new File(filename));
				imgOutputStream.write(bytes);
				imgOutputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			// Free resources.
			image.release();
			inputPointer.deallocate();
			outputPointer.deallocate();
		}
		br.close();

		String[] names = new String[imgFilenames.size()];
		imgFilenames.toArray(names);
		return names;
	}
    
	public static void main(String[] args) throws Exception {
		FtpUtil ftpUtil = FtpUtil.getInstance();
		Ftp ftpProperties = ftpUtil.getFtp();
		FTPClient ftpClient = ftpUtil.getFTPClient(ftpProperties);
		File localFile = new File("E:\\e61190ef76c6a7efa8408794f1faaf51f3de6619.jpg");
		InputStream localInputStream = new FileInputStream(localFile);
		String ftpDir = ftpProperties.getSysFile() + "abc";
		String ftpFileName = new String("hello.jpg".getBytes("UTF-8"), "iso-8859-1");
		if (ftpClient != null && ftpClient.isConnected()) {
			boolean dirExist = ftpUtil.createDir(ftpDir, ftpClient);
			System.out.println("目录创建：" + dirExist);
			if (dirExist) {

				boolean fileExist = ftpUtil.isFTPFileExist(ftpDir, ftpFileName, ftpClient);
				System.out.println("文件是否存在：" + fileExist);
				if (!fileExist) {

					ftpUtil.upload(localInputStream, ftpDir, ftpFileName, ftpClient);
				}
			}

		}
	}
	
  
}  
