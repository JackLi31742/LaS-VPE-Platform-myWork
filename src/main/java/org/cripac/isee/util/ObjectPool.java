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
import org.cripac.isee.alg.pedestrian.attr.DeepMARCaffe2Native;
import org.cripac.isee.alg.pedestrian.attr.DeepMARCaffe2NativeFactory;
import org.cripac.isee.vpe.entities.Report;
import org.cripac.isee.vpe.entities.ServerInfo;
import org.cripac.isee.vpe.util.logging.Logger;

public class ObjectPool  implements Serializable{

    /**
	 * 
	 */
	private static final long serialVersionUID = 2197839630074568045L;
	public KeyedObjectPool<String,DeepMARCaffe2Native> pool = null;
    private static ObjectPool instance = null;
    public static Singleton<Logger> loggerSingleton1=null;
    public static Report report1 =null;
    private ThreadLocal<DeepMARCaffe2Native> tl = new ThreadLocal<DeepMARCaffe2Native>();
    public List<Integer> gpus=new ArrayList<>();
    public ConcurrentHashMap<Integer,Integer> map=new ConcurrentHashMap<Integer, Integer>();
    protected ObjectPool() {
//        GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
//        config.setMaxTotalPerKey(1);
    	GenericKeyedObjectPool.Config conf = new GenericKeyedObjectPool.Config();  
		DeepMARCaffe2NativeFactory deepMARCaffe2NativeFactory=new DeepMARCaffe2NativeFactory(loggerSingleton1);
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
		 GenericKeyedObjectPoolFactory<String,DeepMARCaffe2Native> genericObjectPoolFactory 
		 		= new GenericKeyedObjectPoolFactory<String, DeepMARCaffe2Native>(deepMARCaffe2NativeFactory,conf);  
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

    public static ObjectPool getInstance(Singleton<Logger> loggerSingleton,Report report ) {
        if (instance == null) {
            synchronized (ObjectPool.class) {
                if (instance == null) {
                	loggerSingleton1=loggerSingleton;
                	report1=report;
                    instance = new ObjectPool();
                }
            }
        }
        return instance;
    }
    
    public static ObjectPool getInstance(Singleton<Logger> loggerSingleton ) {
        if (instance == null) {
            synchronized (ObjectPool.class) {
                if (instance == null) {
                	loggerSingleton1=loggerSingleton;
                    instance = new ObjectPool();
                }
            }
        }
        return instance;
    }
    
    public DeepMARCaffe2Native getDeepMARCaffe2Native(String key,KeyedObjectPool<String, DeepMARCaffe2Native> pool1){
//    	KeyedObjectPool<String, DeepMARCaffe2Native> pool=null;
//		try {
//			pool = recognizerSingleton2.getInst();
//		} catch (Exception e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
    	DeepMARCaffe2Native deepMARCaffe2Native = tl.get();
		if (deepMARCaffe2Native != null) {
			if (deepMARCaffe2Native.gpu.equals(key)) {
//				System.out.println("ThreadLocal的hashcode是："+tl.hashCode()+",中的deepMARCaffe2Native的hashcode是："+deepMARCaffe2Native.hashCode()
//					+",使用的GPU是:"+deepMARCaffe2Native.gpu+"传入的key是："+key);
				return deepMARCaffe2Native;
			}else {
				try {
//					int idleNum=pool.getNumIdle();
//					int idleKeyNum=pool.getNumIdle(key);
//					int activeKeyNum=pool.getNumActive(key);
//					int activeNum=pool.getNumActive();
//					System.out.println("activeNum:"+activeNum+",activeKeyNum:"+activeKeyNum+",idleNum:"+idleNum+",idleKeyNum:"+idleKeyNum);
					tl.remove();
					deepMARCaffe2Native = pool1.borrowObject(key);
					tl.set(deepMARCaffe2Native);
//					System.out.println("再次从pool中的deepMARCaffe2Native的hashcode是："+deepMARCaffe2Native.hashCode()
//						+",使用的GPU是:"+deepMARCaffe2Native.gpu+"传入的key是："+key);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return deepMARCaffe2Native;
			}
		} else {
			try {
				deepMARCaffe2Native = pool1.borrowObject(key);
				tl.set(deepMARCaffe2Native);
//				System.out.println("第一次pool中的deepMARCaffe2Native的hashcode是："+deepMARCaffe2Native.hashCode()
//					+",使用的GPU是:"+deepMARCaffe2Native.gpu+"传入的key是："+key);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return deepMARCaffe2Native;
		}
    }
    
    public void releaseDeepMARCaffe2Native(String key,DeepMARCaffe2Native object,KeyedObjectPool<String, DeepMARCaffe2Native> pool1) {
    	DeepMARCaffe2Native deepMARCaffe2Native = tl.get();
		try {
//			if(deepMARCaffe2Native == null) recognizerSingleton2.getInst().returnObject(key,object);
//			if(deepMARCaffe2Native != object) recognizerSingleton2.getInst().returnObject(key,object);
			if (deepMARCaffe2Native==object) {
//				recognizerSingleton2.getInst().returnObject(key,object);
				pool1.returnObject(key,object);
//				tl.remove();
			}else {
				System.out.println("release的对象和当前线程的对象不一样，release的对象的GPU是:"+object.gpu+"当前线程的gpu是："+deepMARCaffe2Native.gpu);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
    
    public void invalidateDeepMARCaffe2Native(String key,DeepMARCaffe2Native object,KeyedObjectPool<String, DeepMARCaffe2Native> pool1){
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