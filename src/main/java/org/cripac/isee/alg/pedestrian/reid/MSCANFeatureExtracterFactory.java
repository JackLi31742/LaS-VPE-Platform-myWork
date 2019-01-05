package org.cripac.isee.alg.pedestrian.reid;

import java.io.Serializable;
import java.util.Collection;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.cripac.isee.util.Singleton;
import org.cripac.isee.vpe.util.logging.Logger;

public class MSCANFeatureExtracterFactory  extends BaseKeyedPoolableObjectFactory<String, MSCANFeatureExtracter> implements Serializable{

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2290511333905175981L;
	public Singleton<Logger> loggerSingleton;
//	public Collection<Integer> gpus;
	
	
	/*public MSCANFeatureExtracterFactory(Singleton<Logger> loggerSingleton, Collection<Integer> gpus) {
		super();
		this.loggerSingleton = loggerSingleton;
		this.gpus = gpus;
	}*/


	public MSCANFeatureExtracterFactory(Singleton<Logger> loggerSingleton) {
		super();
		this.loggerSingleton = loggerSingleton;
	}


	@Override
	public MSCANFeatureExtracter makeObject(String key) throws Exception {
		// TODO Auto-generated method stub
		return new MSCANFeatureExtracter(key, loggerSingleton.getInst());
	}
}
