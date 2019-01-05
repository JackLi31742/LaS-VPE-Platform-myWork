package org.cripac.isee.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.cripac.isee.alg.pedestrian.attr.DeepMARCaffe2Native;
import org.cripac.isee.alg.pedestrian.attr.DeepMARCaffe2NativeFactory;

public class ObjectResourceUtil implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -4013367238719172312L;
	private static ThreadLocal<DeepMARCaffe2Native> tl = new ThreadLocal<DeepMARCaffe2Native>();
//	private static DeepMARCaffe2NativeFactory deepMARCaffe2NativeFactory=DeepMARCaffe2NativeFactory.getDeepMARCaffe2NativeFactory();
//	private static GenericKeyedObjectPool pool = deepMARCaffe2NativeFactory.getGenericKeyedObjectPool();
//	private static KeyedObjectPool<String,DeepMARCaffe2Native> pool = deepMARCaffe2NativeFactory.getGenericKeyedObjectPool();

	
	/*public static DeepMARCaffe2Native getDeepMARCaffe2Native()  {
		
		DeepMARCaffe2Native deepMARCaffe2Native = tl.get();
		if (deepMARCaffe2Native != null) {
			System.out.println("获取对象成功！" + deepMARCaffe2Native.hashCode());
			return deepMARCaffe2Native;
		} else {
			List<Integer> list=new ArrayList<>();
			list.add(0);
			deepMARCaffe2NativeFactory.gpus=list;
			System.out.println("ObjectResourceUtil 的gpu赋值："+deepMARCaffe2NativeFactory.gpus);
			for (int i = 0; i < deepMARCaffe2NativeFactory.gpus.size(); i++) {
				String key = String.valueOf(deepMARCaffe2NativeFactory.gpus.toArray()[i]);
				System.out.println("key:"+key);
				try {
					
					deepMARCaffe2Native = (DeepMARCaffe2Native) pool.borrowObject(key);
					System.out.println("获取对象成功！" + deepMARCaffe2Native.hashCode());
				} catch (Exception ex) {
					try {
						pool.invalidateObject(key, deepMARCaffe2Native);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}  
				}
			}
			return deepMARCaffe2Native;
		}
	}

	public void releaseDeepMARCaffe2Native(DeepMARCaffe2Native deepMARCaffe2NativeOut) {
		DeepMARCaffe2Native deepMARCaffe2Native = tl.get();
		try {
			if (deepMARCaffe2Native == null)
				pool.returnObject(deepMARCaffe2NativeOut.gpu, deepMARCaffe2NativeOut);
			if (deepMARCaffe2Native != deepMARCaffe2NativeOut)
				pool.returnObject(deepMARCaffe2NativeOut.gpu, deepMARCaffe2NativeOut);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}*/
}
