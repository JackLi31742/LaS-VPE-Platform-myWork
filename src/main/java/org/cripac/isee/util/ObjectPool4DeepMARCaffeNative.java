package org.cripac.isee.util;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.cripac.isee.alg.pedestrian.attr.DeepMARCaffeNative;
import org.cripac.isee.alg.pedestrian.attr.DeepMARCaffeNativeFactory;
import org.cripac.isee.vpe.entities.Report;
import org.cripac.isee.vpe.entities.ServerInfo;
import org.cripac.isee.vpe.util.logging.Logger;

public class ObjectPool4DeepMARCaffeNative  implements Serializable{

    /**
	 * 
	 */
	private static final long serialVersionUID = 2197839630074568045L;
	public KeyedObjectPool<String,DeepMARCaffeNative> pool = null;
    private static ObjectPool4DeepMARCaffeNative instance = null;
    public static Singleton<Logger> loggerSingleton1=null;
    public static Report report1 =null;
    private ThreadLocal<DeepMARCaffeNative> tl = new ThreadLocal<DeepMARCaffeNative>();
    public List<Integer> gpus=new ArrayList<>();
    public ConcurrentHashMap<Integer,Integer> map=new ConcurrentHashMap<Integer, Integer>();
    protected ObjectPool4DeepMARCaffeNative() {
//        GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
//        config.setMaxTotalPerKey(1);
    	GenericKeyedObjectPool.Config conf = new GenericKeyedObjectPool.Config();  
		DeepMARCaffeNativeFactory deepMARCaffeNativeFactory=new DeepMARCaffeNativeFactory(loggerSingleton1);
//		System.out.println("DeepMARCaffe2NativeFactory的propCenter:"+propCenter);
//		System.out.println("DeepMARCaffe2NativeFactory的propCenter的gpu:"+gpus);
		/*List<Integer> list=new ArrayList<>();
		list.add(0);
		if (getServerName().equals("gpu-task-nod2")) {
			list.add(1);
		}
		Collection<Integer> gpus=list;*/
//		Collection<Integer> gpus=getGpu();
		gpus=getGpu();
//		System.out.println("赋值后的gpus:"+gpus);
		map=initMap(gpus);
		conf.maxTotal=gpus.size();
		conf.maxIdle=gpus.size();
		conf.maxActive=gpus.size();
		conf.testOnBorrow=true;  
		conf.testOnReturn=true;  
		conf.testWhileIdle=true;  
        conf.lifo = false; 
//		conf.lifo=true;
//		conf.maxWait=600000;
//		conf.minEvictableIdleTimeMillis=100000;
//		conf.minIdle=0;
//		conf.numTestsPerEvictionRun=1;
//		conf.whenExhaustedAction=1;
//		conf.timeBetweenEvictionRunsMillis=600000 ;
//		GenericKeyedObjectPool<String, DeepMARCaffe2Native> pool=
//				new GenericKeyedObjectPool<String, DeepMARCaffe2Native>((KeyedPooledObjectFactory<String, DeepMARCaffe2Native>) new DeepMARCaffe2NativeFactory(),conf);
		 GenericKeyedObjectPoolFactory<String,DeepMARCaffeNative> genericObjectPoolFactory 
		 		= new GenericKeyedObjectPoolFactory<String, DeepMARCaffeNative>(deepMARCaffeNativeFactory,conf);  
	     pool = genericObjectPoolFactory.createPool(); 
	     for (int i = 0; i < 2; i++) {
				try {
					pool.addObject(String.valueOf(gpus.get(i)));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
    }
    
    public synchronized ConcurrentHashMap<Integer,Integer> getMap(){
    	return map;
    }

    public static ObjectPool4DeepMARCaffeNative getInstance(Singleton<Logger> loggerSingleton,Report report ) {
        if (instance == null) {
            synchronized (ObjectPool4DeepMARCaffeNative.class) {
                if (instance == null) {
                	loggerSingleton1=loggerSingleton;
                	report1=report;
                    instance = new ObjectPool4DeepMARCaffeNative();
                }
            }
        }
        return instance;
    }
    
    public static ObjectPool4DeepMARCaffeNative getInstance(Singleton<Logger> loggerSingleton ) {
        if (instance == null) {
            synchronized (ObjectPool4DeepMARCaffeNative.class) {
                if (instance == null) {
                	loggerSingleton1=loggerSingleton;
                    instance = new ObjectPool4DeepMARCaffeNative();
                }
            }
        }
        return instance;
    }
    
    public DeepMARCaffeNative getDeepMARCaffeNative(String key,KeyedObjectPool<String, DeepMARCaffeNative> pool1){
//    	KeyedObjectPool<String, DeepMARCaffe2Native> pool=null;
//		try {
//			pool = recognizerSingleton2.getInst();
//		} catch (Exception e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
    	DeepMARCaffeNative deepMARCaffeNative = tl.get();
		if (deepMARCaffeNative != null) {
			if (deepMARCaffeNative.gpu.equals(key)) {
//				System.out.println("ThreadLocal的hashcode是："+tl.hashCode()+",中的deepMARCaffe2Native的hashcode是："+deepMARCaffe2Native.hashCode()
//					+",使用的GPU是:"+deepMARCaffe2Native.gpu+"传入的key是："+key);
				return deepMARCaffeNative;
			}else {
				try {
//					int idleNum=pool.getNumIdle();
//					int idleKeyNum=pool.getNumIdle(key);
//					int activeKeyNum=pool.getNumActive(key);
//					int activeNum=pool.getNumActive();
//					System.out.println("activeNum:"+activeNum+",activeKeyNum:"+activeKeyNum+",idleNum:"+idleNum+",idleKeyNum:"+idleKeyNum);
					tl.remove();
					deepMARCaffeNative = pool1.borrowObject(key);
					tl.set(deepMARCaffeNative);
//					System.out.println("再次从pool中的deepMARCaffe2Native的hashcode是："+deepMARCaffe2Native.hashCode()
//						+",使用的GPU是:"+deepMARCaffe2Native.gpu+"传入的key是："+key);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return deepMARCaffeNative;
			}
		} else {
			try {
				deepMARCaffeNative = pool1.borrowObject(key);
				tl.set(deepMARCaffeNative);
//				System.out.println("第一次pool中的deepMARCaffe2Native的hashcode是："+deepMARCaffe2Native.hashCode()
//					+",使用的GPU是:"+deepMARCaffe2Native.gpu+"传入的key是："+key);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return deepMARCaffeNative;
		}
    }
    
    public void releaseDeepMARCaffeNative(String key,DeepMARCaffeNative object,KeyedObjectPool<String, DeepMARCaffeNative> pool1) {
    	DeepMARCaffeNative deepMARCaffeNative = tl.get();
		try {
//			if(deepMARCaffe2Native == null) recognizerSingleton2.getInst().returnObject(key,object);
//			if(deepMARCaffe2Native != object) recognizerSingleton2.getInst().returnObject(key,object);
			if (deepMARCaffeNative==object) {
//				recognizerSingleton2.getInst().returnObject(key,object);
				pool1.returnObject(key,object);
//				tl.remove();
			}else {
				System.out.println("release的对象和当前线程的对象不一样，release的对象的GPU是:"+object.gpu+"当前线程的gpu是："+deepMARCaffeNative.gpu);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
    
    public void invalidateDeepMARCaffeNative(String key,DeepMARCaffeNative object,KeyedObjectPool<String, DeepMARCaffeNative> pool1){
    	try {
    		if (object!=null) {
				
//    			recognizerSingleton2.getInst().invalidateObject(key,object);
    			pool1.invalidateObject(key,object);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
    }
    
    /*public Collection<Integer> getGpu(){
    	Map <String,ServerInfo> serverInfosMap=report1.serverInfosMap;
    	String nodeName1=getServerName();
    	if (serverInfosMap.containsKey(nodeName1)) {
    		ServerInfo serverInfo=serverInfosMap.get(nodeName1);
    		List<Integer> devNumList=serverInfo.devNumList;
//    		List<Integer> processNumAllList=serverInfo.processNumAllList;
//    		Collection<Integer> result=CollectionUtils.subtract(devNumList, processNumAllList);  
    		Collection<Integer> result=serverInfo.devNumList;
            result.forEach(f->{System.out.println("服务器上的gpu是："+f);});
            return result;
		}else {
			List<Integer> list=new ArrayList<>();
			list.add(0);
			Collection<Integer> gpus=list;
			return gpus;
		}
    }*/
    
    public List<Integer> getGpu(){
    	Map <String,ServerInfo> serverInfosMap=report1.serverInfosMap;
    	String nodeName1=getServerName();
    	if (serverInfosMap.containsKey(nodeName1)) {
    		ServerInfo serverInfo=serverInfosMap.get(nodeName1);
    		List<Integer> devNumList=serverInfo.devNumList;
//    		List<Integer> processNumAllList=serverInfo.processNumAllList;
//    		Collection<Integer> result=CollectionUtils.subtract(devNumList, processNumAllList);  
//    		Collection<Integer> result=serverInfo.devNumList;
    		
//    		devNumList.forEach(f->{System.out.println("服务器上的gpu是："+f);});
            return devNumList;
		}else {
			List<Integer> list=new ArrayList<>();
			list.add(0);
//			Collection<Integer> gpus=list;
			return list;
		}
    }
    
    public int selectGPURandomly(List<Integer> listCollection) {
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        int pickgpu=listCollection.get(tlr.nextInt(0,listCollection.size()));
        tlr=null;
        return pickgpu;
    }
    
    public ConcurrentHashMap<Integer,Integer> initMap(List<Integer> listCollection){
    	
    	for (int i = 0; i < listCollection.size(); i++) {
			map.put(listCollection.get(i), 0);
		}
    	return map;
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
    public int randomlyPickGPU(List<Integer> listCollection) {
    	Random random = new Random(System.currentTimeMillis());
//    	System.out.println("random的值:"+random.hashCode());
    	int pickgpu=listCollection.get(random.nextInt(listCollection.size()));
    	random=null;
      	return pickgpu;
    }
    
   /* public KeyedObjectPool<String,DeepMARCaffe2Native> getGenericKeyedObjectPool(){
//		GenericKeyedObjectPoolConfig conf=new GenericKeyedObjectPoolConfig();
		GenericKeyedObjectPool.Config conf = new GenericKeyedObjectPool.Config();  
		DeepMARCaffe2NativeFactory deepMARCaffe2NativeFactory=new DeepMARCaffe2NativeFactory();
//		System.out.println("DeepMARCaffe2NativeFactory的propCenter:"+propCenter);
//		System.out.println("DeepMARCaffe2NativeFactory的propCenter的gpu:"+gpus);
		List<Integer> list=new ArrayList<>();
		list.add(0);
		Collection<Integer> gpus=list;
		System.out.println("赋值后的gpus"+gpus);
		conf.maxTotal=(gpus.size());
//		GenericKeyedObjectPool<String, DeepMARCaffe2Native> pool=
//				new GenericKeyedObjectPool<String, DeepMARCaffe2Native>((KeyedPooledObjectFactory<String, DeepMARCaffe2Native>) new DeepMARCaffe2NativeFactory(),conf);
		 GenericKeyedObjectPoolFactory<String,DeepMARCaffe2Native> genericObjectPoolFactory 
		 		= new GenericKeyedObjectPoolFactory<String, DeepMARCaffe2Native>(deepMARCaffe2NativeFactory,conf);  
	     KeyedObjectPool<String, DeepMARCaffe2Native> pool = genericObjectPoolFactory.createPool();  
	     for (int i = 0; i < gpus.size(); i++) {
				String key = String.valueOf(gpus.toArray()[i]) ;
				try {
//					pool.addObject(key);
					pool.borrowObject(key);
				} catch (IllegalStateException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnsupportedOperationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	     }
		return pool;
	}*/

}