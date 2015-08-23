package com.sdw.soft.test.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.junit.Test;

/**
 * Curator demo
 * @author Administrator
 *
 */
public class CuratorDemo {

	/**
	 * Curator 创建一个zookeeper客户端
	 */
	@Test
	public void test01() throws Exception{
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
		CuratorFramework client = CuratorFrameworkFactory.newClient("", retryPolicy);
		client.start();
		Thread.sleep(Integer.MAX_VALUE);
	}
	
	/**
	 * 使用Fluent风格的API创建Zookeeper客户端
	 * @throws Exception
	 */
	@Test
	public void test02() throws Exception{
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
		CuratorFramework client = CuratorFrameworkFactory.builder()
				.connectString("")
				.sessionTimeoutMs(5000)
				.retryPolicy(retryPolicy)
				.build();
		client.start();
		Thread.sleep(Integer.MAX_VALUE);
		
	}
}
