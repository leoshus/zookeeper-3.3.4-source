package com.sdw.soft.test.curator.sharedlock;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author shangyd
 * @date 2015年8月28日 下午6:22:16
 **/
public class FakeLimitedResource {

	private final AtomicBoolean inUse = new AtomicBoolean(false);
	
	public void use()throws InterruptedException{
		if(!inUse.compareAndSet(false, true)){
			throw new IllegalStateException("Needs to be used by one client at a time.");
		}
		try {
			Thread.sleep((long)(3 * Math.random()));
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			inUse.set(false);
		}
	}
}
