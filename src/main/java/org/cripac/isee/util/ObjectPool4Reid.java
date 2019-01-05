package org.cripac.isee.util;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.cripac.isee.alg.pedestrian.reid.MSCANFeatureExtracter;
import org.cripac.isee.alg.pedestrian.reid.MSCANFeatureExtracterFactory;
import org.cripac.isee.vpe.entities.Report;
import org.cripac.isee.vpe.entities.ServerInfo;
import org.cripac.isee.vpe.util.logging.Logger;

public class ObjectPool4Reid  implements Serializable{

    /**
	 * 
	 */
	private static final long serialVersionUID = 2197839630074568045L;
	public KeyedObjectPool<String,MSCANFeatureExtracter> pool = null;
    private static ObjectPool4Reid instance = null;
    public static Singleton<Logger> loggerSingleton1=null;
    public static Report report1 =null;
    private ThreadLocal<MSCANFeatureExtracter> tl = new ThreadLocal<MSCANFeatureExtracter>();
//    Random random = new Random(System.currentTimeMillis());
    public List<Integer> gpus=new ArrayList<>();
    public ConcurrentHashMap<Integer,Integer> map=new ConcurrentHashMap<Integer, Integer>();
//    public ThreadLocal<Integer> countTl=new ThreadLocal<Integer>();
    protected ObjectPool4Reid() {
//        GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
//        config.setMaxTotalPerKey(1);
    	GenericKeyedObjectPool.Config conf = new GenericKeyedObjectPool.Config();  
    	MSCANFeatureExtracterFactory mSCANFeatureExtracterFactory=new MSCANFeatureExtracterFactory(loggerSingleton1);
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
//		GenericKeyedObjectPool<String, DeepMARCaffe2Native> pool=
//				new GenericKeyedObjectPool<String, DeepMARCaffe2Native>((KeyedPooledObjectFactory<String, DeepMARCaffe2Native>) new DeepMARCaffe2NativeFactory(),conf);
		 GenericKeyedObjectPoolFactory<String,MSCANFeatureExtracter> genericObjectPoolFactory 
		 		= new GenericKeyedObjectPoolFactory<String, MSCANFeatureExtracter>(mSCANFeatureExtracterFactory,conf);  
	     pool = genericObjectPoolFactory.createPool(); 
	     for (int i = 0; i < gpus.size(); i++) {
			try {
				pool.addObject(String.valueOf(gpus.get(i)));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    }

    public static ObjectPool4Reid getInstance(Singleton<Logger> loggerSingleton,Report report ) {
        if (instance == null) {
            synchronized (ObjectPool4Reid.class) {
                if (instance == null) {
                	loggerSingleton1=loggerSingleton;
                	report1=report;
                    instance = new ObjectPool4Reid();
                }
            }
        }
        return instance;
    }
    
    public static ObjectPool4Reid getInstance(Singleton<Logger> loggerSingleton ) {
        if (instance == null) {
            synchronized (ObjectPool4Reid.class) {
                if (instance == null) {
                	loggerSingleton1=loggerSingleton;
                    instance = new ObjectPool4Reid();
                }
            }
        }
        return instance;
    }
    
    public MSCANFeatureExtracter getMSCANFeatureExtracter(String key,KeyedObjectPool<String, MSCANFeatureExtracter> pool1){
//    	KeyedObjectPool<String, MSCANFeatureExtracter> pool=null;
//		try {
//			pool = reidSingleton2.getInst();
//		} catch (Exception e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
    	MSCANFeatureExtracter mSCANFeatureExtracter = tl.get();
    	
//    	MSCANFeatureExtracter mSCANFeatureExtracter = null;
		if (mSCANFeatureExtracter != null) {
			if (mSCANFeatureExtracter.gpu.equals(key)) {
//				System.out.println("ThreadLocal的hashcode是："+tl.hashCode()+",中的mSCANFeatureExtracter的hashcode是："+mSCANFeatureExtracter.hashCode()
//						+",使用的GPU是:"+mSCANFeatureExtracter.gpu+"传入的key是："+key);
				
				return mSCANFeatureExtracter;
			}else {
				try {
//					int idleNum=pool.getNumIdle();
//					int idleKeyNum=pool.getNumIdle(key);
//					int activeKeyNum=pool.getNumActive(key);
//					int activeNum=pool.getNumActive();
//					System.out.println("activeNum:"+activeNum+",activeKeyNum:"+activeKeyNum+",idleNum:"+idleNum+",idleKeyNum:"+idleKeyNum);
//					if (idleNum>=0&&idleNum<=gpus.size()
//							&&idleKeyNum>=0&&idleKeyNum<=1
//							&&activeKeyNum>=0&&activeKeyNum<1
//							&&activeNum>=0&&activeNum<gpus.size()) {
						tl.remove();
						mSCANFeatureExtracter = pool1.borrowObject(key);
						tl.set(mSCANFeatureExtracter);
//						System.out.println("再次从pool中的mSCANFeatureExtracter的hashcode是："+mSCANFeatureExtracter.hashCode()
//							+",使用的GPU是:"+mSCANFeatureExtracter.gpu+"传入的key是："+key);
//					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return mSCANFeatureExtracter;
			}
		} else {
			try {
				mSCANFeatureExtracter = pool1.borrowObject(key);
				tl.set(mSCANFeatureExtracter);
//				System.out.println("第一次pool中的mSCANFeatureExtracter的hashcode是："+mSCANFeatureExtracter.hashCode()
//					+",使用的GPU是:"+mSCANFeatureExtracter.gpu+"传入的key是："+key);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return mSCANFeatureExtracter;
		}
    }
    
    public void releaseMSCANFeatureExtracter(String key,MSCANFeatureExtracter object,KeyedObjectPool<String, MSCANFeatureExtracter> pool1) {
    	MSCANFeatureExtracter mSCANFeatureExtracter = tl.get();
		try {
//			if(mSCANFeatureExtracter == null) pool.returnObject(key,object);
//			if(mSCANFeatureExtracter != object) pool.returnObject(key,object);
			if (mSCANFeatureExtracter==object) {
//				reidSingleton2.getInst().returnObject(key,object);
				pool1.returnObject(key,object);
//				tl.remove();
			}else {
				System.out.println("release的对象和当前线程的对象不一样，release的对象的GPU是:"+object.gpu+"当前线程的gpu是："+mSCANFeatureExtracter.gpu);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
    
    public void invalidateMSCANFeatureExtracter(String key,MSCANFeatureExtracter object,KeyedObjectPool<String, MSCANFeatureExtracter> pool1){
    	try {
    		if (object!=null) {
				
//    			reidSingleton2.getInst().invalidateObject(key,object);
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
    
    /** 
     * Select a gpu randomly.
     */
    public int selectGPURandomly(String gpus) {
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        String[] gpuIDs = gpus.split(",");
        return Integer.parseInt(gpuIDs[tlr.nextInt(0, gpuIDs.length)]);
    }
    
    /** 
     * Select a gpu randomly.
     */
    public synchronized int selectGPURandomly(List<Integer> listCollection) {
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
    
    public synchronized Integer pickGpu(ConcurrentHashMap<Integer, Integer> map, int value) {
    	Integer pickGpu = null;
		for (Integer getKey : map.keySet()) {
//		Random random = new Random(System.currentTimeMillis());
//			int size=map.size();
//			int count=0;
//			while (count<size) {
//				
//				Integer getKey=random.nextInt(size);
				if (map.get(getKey) == 0) {
					pickGpu = getKey;
					break;
				}
//				count++;
			}
//			random=null;
		if ( pickGpu!=null) {
			
			map.replace(pickGpu,0, 1); 
		}else {
			System.out.println("key是空的");
		}
		return pickGpu;
	}
    
	public void updateMap(ConcurrentHashMap<Integer, Integer> map,int key){
		map.replace(key, 0);
	}
	
	public synchronized int pickGpuByList(List<Integer> listCollection,int a){
//		int a=countTl.get();
		int gpu=listCollection.get(a%listCollection.size());
//		a=a+listCollection.size();
		return gpu;
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