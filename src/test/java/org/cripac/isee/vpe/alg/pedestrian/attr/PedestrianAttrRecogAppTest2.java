package org.cripac.isee.vpe.alg.pedestrian.attr;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.cripac.isee.alg.pedestrian.attr.Attributes;
import org.cripac.isee.alg.pedestrian.attr.DeepMARCaffe2Native;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.util.ObjectPool;
import org.cripac.isee.util.Singleton;
import org.cripac.isee.vpe.alg.pedestrian.tracking.TrackletOrURL;
import org.cripac.isee.vpe.ctrl.GpuManage;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.entities.Report;
import org.cripac.isee.vpe.util.hdfs.HadoopHelper;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;

public class PedestrianAttrRecogAppTest2 {
	
//	static Logger logger= Logger.getLogger(PedestrianAttrRecogAppTest2.class);
	 private static Singleton<KeyedObjectPool<String,DeepMARCaffe2Native>> recognizerSingleton2;
     private static Singleton<ObjectPool> recognizerSingleton3;
     private static  Singleton<Logger> loggerSingleton;
     private static  Singleton<Report>  reportSingleton ;
     
//     static DeepMARCaffe2Native deepMARCaffe2Native =null;
     public static void init(String[] args)throws Exception{
    	 String appName="test";
 		SystemPropertyCenter propCenter=new SystemPropertyCenter(args);
 		loggerSingleton = new Singleton<>(new SynthesizedLoggerFactory(appName, propCenter),SynthesizedLogger.class);

         reportSingleton=new Singleton<>(()->new GpuManage().get(),Report.class);
 		recognizerSingleton3= new Singleton<>(() -> ObjectPool.getInstance(loggerSingleton,reportSingleton.getInst()),ObjectPool.class);
         recognizerSingleton2 = new Singleton<>(() -> recognizerSingleton3.getInst().pool,KeyedObjectPool.class);
     }
	public static void main(String[] args) throws Exception {
		System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism","3");
		init(args);
		
		List<Path> filenames = null;
		try {
			filenames = getFileList("/user/vpetest.cripac/source_data/video/CAM01/2013-12-23");
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		for (int i = 0; i < filenames.size(); i++) {
			
        	System.out.println("视频路径是："+i+":"+filenames.get(i).toString());
        	
		}
//		final DeepMARCaffe2Native deepMARCaffe2Native0 =new DeepMARCaffe2Native(String.valueOf(0),loggerSingleton.getInst());
//		final DeepMARCaffe2Native deepMARCaffe2Native1 =new DeepMARCaffe2Native(String.valueOf(1),loggerSingleton.getInst());
//		final DeepMARCaffe2Native deepMARCaffe2Native2 =new DeepMARCaffe2Native(String.valueOf(2),loggerSingleton.getInst());
//		final DeepMARCaffe2Native deepMARCaffe2Native3 =new DeepMARCaffe2Native(String.valueOf(3),loggerSingleton.getInst());
		for (int i = 7; i < 8; i++) {
			Path path=filenames.get(i);

            String videoName=path.getName().substring(0, path.getName().lastIndexOf('.'));
            List<String> list=HadoopHelper.getTrackletUrl(videoName);
//            list.forEach(f->{
//            	TrackletOrURL url = new TrackletOrURL(f);
//            	Tracklet tracklet;
//				try {
//					tracklet = url.getTracklet();
//					System.out.println("未解析前："+tracklet.id);
//					Attributes attr = deepMARCaffe2Native0.recognize(tracklet);
//					System.out.println("解析后：gpu:"+deepMARCaffe2Native0.gpu+",id:"+attr.trackletID+","+attr.genderMale);
//				} catch (Exception e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//            });
            Stream<String> stream=list.stream();
            stream.parallel().forEach(f->{
            	String key =String.valueOf(0) ;
            	
				if (key!=null) {
					
            	
            	TrackletOrURL url = new TrackletOrURL(f);
            	try {
            		final DeepMARCaffe2Native deepMARCaffe2Native =new DeepMARCaffe2Native(key,loggerSingleton.getInst());
					Tracklet tracklet=url.getTracklet();
					System.out.println("未解析前："+tracklet.id);
					Attributes attr = deepMARCaffe2Native.recognize(tracklet);
					System.out.println("解析后：gpu:"+deepMARCaffe2Native.gpu+",id:"+attr.trackletID+","+attr.genderMale);
					deepMARCaffe2Native.free(deepMARCaffe2Native.net);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Throwable e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
				}
            	
            });
            synchronized (path) {
            	
            	Thread.sleep(60);
            }
	
		}
		
		for (int i = 3; i < 4; i++) {
			Path path=filenames.get(i);
			
			String videoName=path.getName().substring(0, path.getName().lastIndexOf('.'));
			List<String> list=HadoopHelper.getTrackletUrl(videoName);
//			list.forEach(f->{
//            	TrackletOrURL url = new TrackletOrURL(f);
//            	Tracklet tracklet;
//				try {
//					tracklet = url.getTracklet();
//					System.out.println("未解析前："+tracklet.id);
//					Attributes attr = deepMARCaffe2Native1.recognize(tracklet);
//					System.out.println("解析后：gpu:"+deepMARCaffe2Native0.gpu+",id:"+attr.trackletID+","+attr.genderMale);
//				} catch (Exception e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//            });
			Stream<String> stream=list.stream();
			stream.parallel().forEach(f->{
				String key =String.valueOf(1) ;
				
				
				if (key!=null) {
					
					
					TrackletOrURL url = new TrackletOrURL(f);
					try {
						final DeepMARCaffe2Native deepMARCaffe2Native =new DeepMARCaffe2Native(key,loggerSingleton.getInst());
						Tracklet tracklet=url.getTracklet();
						System.out.println("未解析前："+tracklet.id);
						Attributes attr = deepMARCaffe2Native.recognize(tracklet);
						System.out.println("解析后：gpu:"+deepMARCaffe2Native.gpu+",id:"+attr.trackletID+","+attr.genderMale);
						deepMARCaffe2Native.free(deepMARCaffe2Native.net);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Throwable e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				
			});
			synchronized (path) {
				
				Thread.sleep(60);
			}
			
		}
		for (int i = 0; i < 1; i++) {
			Path path=filenames.get(i);
			
			String videoName=path.getName().substring(0, path.getName().lastIndexOf('.'));
			List<String> list=HadoopHelper.getTrackletUrl(videoName);
//			list.forEach(f->{
//            	TrackletOrURL url = new TrackletOrURL(f);
//            	Tracklet tracklet;
//				try {
//					tracklet = url.getTracklet();
//					System.out.println("未解析前："+tracklet.id);
//					Attributes attr = deepMARCaffe2Native2.recognize(tracklet);
//					System.out.println("解析后：gpu:"+deepMARCaffe2Native0.gpu+",id:"+attr.trackletID+","+attr.genderMale);
//				} catch (Exception e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//            });
			Stream<String> stream=list.stream();
			stream.parallel().forEach(f->{
				String key =String.valueOf(3) ;
				
				
				if (key!=null) {
					
					
					TrackletOrURL url = new TrackletOrURL(f);
					try {
						final DeepMARCaffe2Native deepMARCaffe2Native =new DeepMARCaffe2Native(key,loggerSingleton.getInst());
						Tracklet tracklet=url.getTracklet();
						System.out.println("未解析前："+tracklet.id);
						Attributes attr = deepMARCaffe2Native.recognize(tracklet);
						System.out.println("解析后：gpu:"+deepMARCaffe2Native.gpu+",id:"+attr.trackletID+","+attr.genderMale);
						deepMARCaffe2Native.free(deepMARCaffe2Native.net);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Throwable e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				
			});
			synchronized (path) {
				
				Thread.sleep(60);
			}
			
		}
		
	}
	public void usePool(String[] args)throws Exception{

		System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism","2");
		init(args);
		
		List<Path> filenames = null;
		try {
			filenames = getFileList("/user/vpetest.cripac/source_data/video/CAM01/2013-12-23");
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		for (int i = 0; i < filenames.size(); i++) {
			
        	System.out.println("视频路径是："+i+":"+filenames.get(i).toString());
        	
		}
		
		
//		String gpu = "0";
//        DeepMARCaffe2Native recognizer = new DeepMARCaffe2Native(gpu,new ConsoleLogger(Level.DEBUG));
		
		final ObjectPool objectPool = recognizerSingleton3.getInst();
//		try {
//			objectPool = recognizerSingleton3.getInst();
//		} catch (Exception e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
		final KeyedObjectPool<String, DeepMARCaffe2Native> pool = recognizerSingleton2.getInst();
//		try {
//			pool = recognizerSingleton2.getInst();
//		} catch (Exception e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
		filenames.forEach(path -> {
            String videoName=path.getName().substring(0, path.getName().lastIndexOf('.'));
            List<String> list=HadoopHelper.getTrackletUrl(videoName);
            Stream<String> stream=list.stream();
            stream.parallel().forEach(f->{
            	ConcurrentHashMap<Integer,Integer> map=objectPool.getMap();
            	ThreadLocal<Integer> tl = new ThreadLocal<Integer>();
            	Integer pickGpu = null;
        		if (tl.get()==null) {
        			
        			for (Integer getKey : map.keySet()) {
        					if (map.get(getKey) == 0) {
        						pickGpu = getKey;
        						tl.set(pickGpu);
        						break;
        					}
        				}
        			if ( pickGpu!=null) {
        				
        				map.replace(pickGpu,0, 1); 
        			}else {
        				System.out.println("key是空的");
        			}
        		}else {
        			pickGpu=tl.get();
        		}
            	
            	String key =String.valueOf(pickGpu) ;
            	
				if (key!=null) {
					
            	DeepMARCaffe2Native deepMARCaffe2Native =objectPool.getDeepMARCaffe2Native(key, pool);
            	TrackletOrURL url = new TrackletOrURL(f);
            	try {
					Tracklet tracklet=url.getTracklet();
					System.out.println("未解析前："+tracklet.id);
					Attributes attr = deepMARCaffe2Native.recognize(tracklet);
					System.out.println("解析后：gpu:"+deepMARCaffe2Native.gpu+",id:"+attr.trackletID+","+attr.genderMale);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
            	 try {  
							objectPool.releaseDeepMARCaffe2Native(key, deepMARCaffe2Native, pool);
					} catch (Exception e) {  
						e.printStackTrace();  
					}finally {
						map.replace(pickGpu,1,0);
					}
				}
            	
            });
//            for (int i = 0; i < list.size(); i++) {
//            	TrackletOrURL url = new TrackletOrURL(list.get(i));
//            	try {
//					Tracklet tracklet=url.getTracklet();
//					System.out.println("未解析前："+tracklet.id);
//					Thread t1=new Thread(new PedestrianAttrRecogAppTest2()); 
//					Thread t2=new Thread(new PedestrianAttrRecogAppTest2()); 
//					t1.start();
//					t2.start();
//					deepMARCaffe2Native =objectPool.getDeepMARCaffe2Native(key, pool);
//					Attributes attr = deepMARCaffe2Native.recognize(tracklet);
//					System.out.println("解析后：gpu:"+deepMARCaffe2Native.gpu+",id:"+attr.trackletID+","+attr.genderMale);
//				} catch (Exception e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
	});
		 

	
	}
	private static List<Path>  getFileList(String path) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        String hadoopHome = System.getenv("HADOOP_HOME");
        
        FileSystem hdfs = FileSystem.get(new URI("hdfs://cpu-master-nod:8020"), conf);
        List<Path> filesAllList = new ArrayList<Path>();
        Path s_path = new Path(path);

       /* if (hdfs.exists(s_path)) {
        	 FileStatus[] fs =hdfs.listStatus(s_path);
        	 Path[] paths = FileUtil.stat2Paths(fs);
//            for (FileStatus status:hdfs.listStatus(s_path)) {
//        		 files.add(status.getPath().toString());
//            }
        	 for (int i = 0; i < paths.length; i++) {
				
        		 files.add(paths[i].toString());
			}
        } else {
            System.out.println("Hey guys, you are wrong!");
        }*/
        traverseFolder1(s_path, hdfs,filesAllList);
        hdfs.close();

        return filesAllList;
    }
    /**
     * 递归打印目录下所有文件和文件夹
     * LANG
     * @param path
     * @param hdfs
     * @param filesAllList
     * @throws Exception
     */
    public static void traverseFolder1(Path path,FileSystem hdfs,List<Path> filesAllList) throws Exception{
		int eachFileNum = 0, eachDirNum = 0;
		if (hdfs.exists(path)) {
			List<String> filesEachList = new ArrayList<String>();
			FileStatus[] fs =hdfs.listStatus(path);
       	 	Path[] paths = FileUtil.stat2Paths(fs);
       	 	for (int i = 0; i < paths.length; i++) {
       		
				if (hdfs.isDirectory(paths[i])) {
					System.out.println("文件夹:" + paths[i].toString());
					traverseFolder1(paths[i],hdfs,filesAllList);
					eachDirNum++;
				} else {
					System.out.println("文件:" + paths[i].toString());
					filesEachList.add(paths[i].toString());
					filesAllList.add(paths[i]);
					eachFileNum++;
				}
			}
		}
		System.out.println("文件夹共有:" + eachDirNum + ",文件共有:" + eachFileNum);

	}
//	@Override
//	public void run() {
//		// TODO Auto-generated method stub
//		
//    	ThreadLocal<Integer> tl = new ThreadLocal<Integer>();
//    	
//    	
//    	
//    	Integer pickGpu = null;
//		if (tl.get()==null) {
//			
//			for (Integer getKey : map.keySet()) {
//					if (map.get(getKey) == 0) {
//						pickGpu = getKey;
//						tl.set(pickGpu);
//						break;
//					}
//				}
//			if ( pickGpu!=null) {
//				
//				map.replace(pickGpu,0, 1); 
//			}else {
//				System.out.println("key是空的");
//			}
//		}else {
//			pickGpu=tl.get();
//		}
//    	
//    	String key =String.valueOf(pickGpu) ;
//    	
//    	
//    	
//    	
//    	
//	}
}
