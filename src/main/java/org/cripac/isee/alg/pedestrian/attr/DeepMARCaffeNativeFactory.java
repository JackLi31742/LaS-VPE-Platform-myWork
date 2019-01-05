package org.cripac.isee.alg.pedestrian.attr;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.cripac.isee.util.Singleton;
import org.cripac.isee.vpe.util.logging.Logger;

public class DeepMARCaffeNativeFactory extends BaseKeyedPoolableObjectFactory<String, DeepMARCaffeNative> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8207477341113111121L;
//	public AppPropertyCenter propCenter;
	public Singleton<Logger> loggerSingleton;
//	public Collection<Integer> gpus;
//	private Singleton<DeepMARCaffe2Native> recognizerSingleton;
//	private static ThreadLocal<DeepMARCaffe2Native> tl = new ThreadLocal<DeepMARCaffe2Native>();
//	public Logger logger;
//	private DeepMARCaffe2NativeFactory(){}
//	private static DeepMARCaffe2NativeFactory deepMARCaffe2NativeFactory=new DeepMARCaffe2NativeFactory();
//	public static DeepMARCaffe2NativeFactory getDeepMARCaffe2NativeFactory(){
//		return deepMARCaffe2NativeFactory;
//	}
	
	
	
/*	public void init(Collection<Integer> gpus,Singleton<Logger> loggerSingleton) throws Exception{
//		this.propCenter=propCenter;
		System.out.println("传入DeepMARCaffe2NativeFactory的参数:"+loggerSingleton+","+gpus);
		this.loggerSingleton=loggerSingleton;
		this.gpus=gpus;
		System.out.println("DeepMARCaffe2NativeFactory的参数："+this.gpus);
//		this.logger=loggerSingleton.getInst();
	}*/
	
	/*public DeepMARCaffe2NativeFactory( Collection<Integer> gpus,Singleton<Logger> loggerSingleton) {
	super();
	this.loggerSingleton = loggerSingleton;
	this.gpus = gpus;
}*/

	
	public DeepMARCaffeNativeFactory(Singleton<Logger> loggerSingleton) {
	super();
	this.loggerSingleton = loggerSingleton;
}


	@Override
	public DeepMARCaffeNative makeObject(String gpu) throws Exception {
		// TODO Auto-generated method stub
		DeepMARCaffeNative deepMARCaffeNative=new DeepMARCaffeNative(gpu,loggerSingleton.getInst());
		return deepMARCaffeNative;
	}
	
	/*public DeepMARCaffe2Native makeObject(String gpu) throws Exception {
		// TODO Auto-generated method stub
		DeepMARCaffe2Native deepMARCaffe2Native = tl.get();// 获取当前线程的事务连接
		if (deepMARCaffe2Native != null) {
			System.out.println("deepMARCaffe2Native的hashcode是："+deepMARCaffe2Native.hashCode());
			return deepMARCaffe2Native;
		} else {
			try {
				deepMARCaffe2Native = new DeepMARCaffe2Native(gpu,loggerSingleton.getInst());
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("deepMARCaffe2Native的hashcode是："+deepMARCaffe2Native.hashCode());
			return deepMARCaffe2Native;
		}
//		recognizerSingleton = new Singleton<>(() -> new DeepMARCaffe2Native(gpu, loggerSingleton.getInst()),DeepMARCaffe2Native.class);
//		DeepMARCaffe2Native deepMARCaffe2Native=recognizerSingleton.getInst();
//		System.out.println("deepMARCaffe2Native的hashcode是："+deepMARCaffe2Native.hashCode());
//		return deepMARCaffe2Native;
	}*/
	
	
	/*public void test() {
		GenericKeyedObjectPoolConfig conf=new GenericKeyedObjectPoolConfig();
		conf.setMaxTotal(propCenter.gpus.size());
		GenericKeyedObjectPool<String, DeepMARCaffe2Native> pool=
				new GenericKeyedObjectPool<String, DeepMARCaffe2Native>((KeyedPooledObjectFactory<String, DeepMARCaffe2Native>) new DeepMARCaffe2NativeFactory(),conf);
		
		for (int i = 0; i < propCenter.gpus.size(); i++) {
			String key=(String)propCenter.gpus.toArray()[i];
            DeepMARCaffe2Native deepMARCaffe2Native = null;  
            try {  
            	deepMARCaffe2Native = (DeepMARCaffe2Native)pool.borrowObject(key);
//            	deepMARCaffe2Native.toString();
            } catch(Exception ex) {  
                try {
					pool.invalidateObject(key, deepMARCaffe2Native);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}  
            } finally {  
                try{
                    if (deepMARCaffe2Native != null) {
                        pool.returnObject(key, deepMARCaffe2Native);
                    }
//                    pool.close();
                }
                catch (Exception e){
                    e.printStackTrace();
                }
            }  
        } 
	}*/
}
