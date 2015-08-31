package com.sdw.soft.test.curator.sharedlock;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

/**
 * @author shangyd
 * @date 2015年8月28日 下午6:25:28
 **/
public class SharedLockDemo {

	private final InterProcessMutex lock;
	private final FakeLimitedResource resource;
	private final String clientName;
	
	public SharedLockDemo(CuratorFramework client,String path,FakeLimitedResource resource,String clientName){
		this.resource = resource;
		this.clientName = clientName;
		lock = new InterProcessMutex(client, path);
	}
	
	
	public void doWork(long time,TimeUnit unit)throws Exception{
		if(!lock.acquire(time,unit)){
			throw new IllegalStateException(clientName + " could not acquire the lock");
		}
		try {
			System.out.println(clientName + "has the lock.");
			resource.use();
		}finally{
			System.out.println(clientName + " releasing the lock.");
			lock.release();
		}
	}
}
